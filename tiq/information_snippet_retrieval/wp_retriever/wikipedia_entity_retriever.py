import os
import pickle
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests
import spacy
from bs4 import BeautifulSoup
from filelock import FileLock

from tiq.information_snippet_retrieval.wp_retriever.entity_evidence_annotator import EvidenceAnnotator
from tiq.information_snippet_retrieval.wp_retriever.infobox_parser import (
    InfoboxParser,
    infobox_to_evidences,
)
from tiq.information_snippet_retrieval.wp_retriever.text_parser import (
    extract_text_snippets,
)
from tiq.library.temporal_expression import TemporalExpression
from tiq.library.utils import get_logger
from tiq.library.wikipedia_library import _wiki_path_to_title, format_wiki_path, \
    is_wikipedia_path, _wiki_title_to_path

ENT_PATTERN = re.compile("^Q[0-9]+$")
MY_PATTERN = re.compile(
    "\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}\b")
API_URL = "http://en.wikipedia.org/w/api.php"
PARAMS = {
    "prop": "extracts|revisions",
    "format": "json",
    "action": "query",
    "explaintext": "",
    "rvprop": "content",
}


class WikipediaEntityPageRetriever:
    """
    Variant of the Wikipedia retrieval for entities.
    """

    def __init__(self, config, clocq, wikidata_mappings, wikipedia_mappings):
        self.config = config
        self.logger = get_logger(__name__, config)
        self.use_cache = False
        self.data_path = self.config["data_path"]
        self.wikipedia_dump_file = self.config["wikipedia_dump_file"]
        self.path_to_dump = os.path.join(self.data_path, self.wikipedia_dump_file)

        self.clocq = clocq
        self.wikidata_mappings = wikidata_mappings
        self.wikipedia_mappings = wikipedia_mappings
        self.max_non_date_entity_in_text = self.config["max_non_date_entity_in_text"]  # maximum non-date entity in text
        # load nlp pipeline
        self.nlp = spacy.blank("en")
        self.nlp.add_pipe("sentencizer")
        self.logger.debug("WikipediaRetriever successfully initialized!")
        self.entity_type_map = {}
        if self.use_cache:
            self._init_wikipediaentity_dump()
            self.dump_changed = False

        self.temporal_expression = TemporalExpression(config)
        # initialize evidence annotator (used for (text)->Wikipedia->Wikidata)
        self.annotator = EvidenceAnnotator(config, self.temporal_expression, self.wikidata_mappings)

    def _filter_noise_entity_evidence(self, evidence):
        non_date_entities = []

        if "explicit_expression" not in evidence:
            self.logger.info(f"The evidence has no explicit expression! ", evidence)
            return False

        explicit_expression = evidence["explicit_expression"]
        timespans = []
        signal_flag = 0

        for item in explicit_expression:
            if item["signal"] == "BEFORE" or item["signal"] == "AFTER" or item["signal"] == "START" or item[
                "signal"] == "FINISH":
                signal_flag = 1
            if item["timespan"] and item["timespan"] not in timespans:
                # check if there is duplicated dates
                timespans.append(item["timespan"])

        # only keep the evidence having one timespan
        if evidence["source"] == "text":
            if len(timespans) == 0 or len(timespans) > 1:
                return False
            # only keep the evidence having overlap or no signal temporal relation
            if signal_flag == 1:
                return False
        else:
            if len(timespans) == 0 or len(timespans) > 2:
                return False

        for item in evidence["wikidata_entities"]:
            if ENT_PATTERN.match(item["id"]):
                # QID
                if item["id"] in self.entity_type_map:
                    entity_type = self.entity_type_map[item["id"]]
                else:
                    entity_type = self.clocq.get_type(item["id"])
                    self.entity_type_map[item["id"]] = entity_type
                if entity_type:
                    item["type"] = entity_type["label"]
                    type = entity_type["label"].lower()
                    # remove date entities
                    if not ("calendar" in type or "year" in type or "time" in type or "day" in type):
                        non_date_entities.append(item)
                else:
                    item["type"] = "NULL"
                    non_date_entities.append(item)

        if len(non_date_entities) == 0 or len(non_date_entities) > self.max_non_date_entity_in_text:
            return False

        return non_date_entities

    def _filter_noise_year_evidence(self, evidence):
        non_date_entities = []
        timespans = []
        signal_flag = 0

        if "explicit_expression" not in evidence:
            self.logger.info(f"The evidence has no explicit expression! ", evidence)
            return False

        for item in evidence["explicit_expression"]:
            if item["signal"] == "BEFORE" or item["signal"] == "AFTER" or item["signal"] == "START" or item[
                "signal"] == "FINISH":
                signal_flag = 1
            if item["timespan"] and item["timespan"] not in timespans:
                # check if there is duplicated dates
                timespans.append(item["timespan"])

        # drop the text having more than one timespans
        if not len(timespans) == 1:
            return False

        # drop the text having before, after, ... temporal signals, only keep overlap or no signal
        if signal_flag == 1:
            return False

        if "wikidata_entities" not in evidence:
            return False

        for item in evidence["wikidata_entities"]:
            if ENT_PATTERN.match(item["id"]):
                # QID
                if item["id"] in self.entity_type_map:
                    entity_type = self.entity_type_map[item["id"]]
                else:
                    entity_type = self.clocq.get_type(item["id"])
                    self.entity_type_map[item["id"]] = entity_type
                if entity_type:
                    # add type label to entity
                    item["type"] = entity_type["label"]
                    type = entity_type["label"].lower()
                    # remove date entities
                    if not ("calendar" in type or "year" in type or "time" in type or "day" in type):
                        non_date_entities.append(item)
                else:
                    item["type"] = "NULL"
                    non_date_entities.append(item)

        # drop the text without at least one entity.
        if len(non_date_entities) == 0:
            return False

        return non_date_entities

    def year_evidences_selection(self, evidences):
        # prune evidences with more than one timespan etc
        selected_evidences = []
        for evidence in evidences:
            # check the number of dates in each evidence text meanwhile adding the type for each entity
            non_date_entities = self._filter_noise_year_evidence(evidence)
            if non_date_entities:
                if evidence not in selected_evidences:
                    # remove timestamps from wikidata_entities
                    evidence["wikidata_entities"] = non_date_entities
                    selected_evidences.append(evidence)
        return selected_evidences

    def extract_content_from_html(self, html):
        if html is None:
            return None

        soup = BeautifulSoup(html, "html.parser")
        content_div = soup.find("div", {"id": "mw-content-text"})

        if content_div:
            return content_div.get_text()
        else:
            print("Content extraction failed.")
            return None

    def _retrieve_event_markdown(self, wiki_title):
        base_url = "https://en.wikipedia.org/wiki/"
        link = f"{base_url}{wiki_title}"
        try:
            response = requests.get(link)
            soup = BeautifulSoup(response.content, features="html.parser")
            content_div = soup.find("div", {"id": "mw-content-text"})
            if content_div:
                return content_div.contentsget_text()
        except:
            return None

    def _build_event_document_anchor_dict(self, soup):
        # prune navigation bar
        for div in soup.find_all("div", {"class": "navbox"}):
            div.decompose()

        # go through links
        anchor_dict = dict()
        for tag in soup.find_all("a"):
            # anchor text
            text = tag.text.strip()
            if len(text) < 3:
                continue
            # duplicate anchor text (keep first)
            # -> later ones can be more specific/incorrect
            if anchor_dict.get(text):
                continue

            # wiki title (=entity)
            href = tag.attrs.get("href")
            if href:
                wiki_path = href.replace("/wiki/", "")

                anchor_dict[text] = wiki_path
        return anchor_dict

    def wp_event_retriever(self, year_id_path_lable):
        """
                Retrieve events from Wikipedia for the given Wikipedia title.
                Always returns the event wikipedia links, wikidata qids
                """
        # retrieve Wikipedia soup
        # {'id': wikidata_id, 'wiki_path':month_wiki_path, 'label': month_wiki_label}
        wikidata_id = year_id_path_lable["id"]
        wiki_path = year_id_path_lable["wiki_path"]

        if self.use_cache and wikidata_id in self.wikipedia_dump:
            self.logger.debug(f"Found Wikipedia evidences in dump!")
            wikidata_entities = self.wikipedia_dump.get(wikidata_id)

        else:

            # get Wikipedia title
            wiki_title = _wiki_path_to_title(wiki_path)
            # retrieve Wikipedia soup
            soup = self._retrieve_soup(wiki_title)

            if soup is None:
                if self.use_cache:
                    self.wikipedia_dump[wikidata_id] = []  # remember
                return []

            # extract anchors
            doc_anchor_dict = self._build_event_document_anchor_dict(soup)
            wikidata_entities = self.annotator.annotate_wikidata_events(wiki_title, doc_anchor_dict)

            if self.use_cache and wikidata_id not in self.wikipedia_dump:
                self.wikipedia_dump[wikidata_id] = wikidata_entities
                self.dump_changed = True

        self.logger.debug(f"Entities on the event page successfully retrieved for {year_id_path_lable}.")
        self.logger.debug(f"Number of Entities on the event page: {len(wikidata_entities)}.")
        return wikidata_entities

    def wp_year_retriever(self, year_id_path_lable):
        """
        Retrieve evidences from Wikipedia for the given Wikipedia title.
        Always returns the full set of evidences (text, table, infobox).
        Filtering is done via filter_evidences function.
        """
        # retrieve Wikipedia soup
        # {'id': wikidata_id, 'wiki_path':month_wiki_path, 'label': month_wiki_label}
        wikidata_id = year_id_path_lable["id"]
        wiki_path = year_id_path_lable["wiki_path"]

        entities = []

        if self.use_cache and wikidata_id in self.wikipedia_dump:
            self.logger.debug(f"Found Wikipedia evidences in dump!")
            text_snippets = self.wikipedia_dump.get(wikidata_id)

        else:
            # get Wikipedia title
            wiki_title = _wiki_path_to_title(wiki_path)
            # retrieve Wikipedia soup
            print("wiki_title")
            print(wiki_title)
            soup = self._retrieve_soup(wiki_title)

            if soup is None:
                if self.use_cache:
                    self.wikipedia_dump[wikidata_id] = []  # remember
                return [], entities

            # retrieve Wikipedia markdown
            wiki_md = self._retrieve_markdown(wiki_title)

            # extract anchors
            doc_anchor_dict = self._build_document_anchor_dict(soup)

            # retrieve evidences
            text_snippets = self._retrieve_text_snippets(wiki_title, wiki_md)

            for evidence in text_snippets:
                evidence["index"] = text_snippets.index(evidence)
                evidence["retrieved_for_entity"] = year_id_path_lable

            self.annotator.annotate_wikidata_entities(wiki_title, text_snippets, doc_anchor_dict)

            if self.use_cache and wikidata_id not in self.wikipedia_dump:
                self.wikipedia_dump[wikidata_id] = text_snippets
                self.dump_changed = True

        for item in text_snippets:
            if "wikidata_entities" not in item:
                self.logger.info(f"Evidence has no wikidata_entities!!: {item}")
                continue
            for item in item["wikidata_entities"]:
                if ENT_PATTERN.match(item["id"]) and item not in entities:
                    entities.append(item)

        self.logger.debug(f"Evidences successfully retrieved for {year_id_path_lable}.")
        text_evidences = self.year_evidences_selection(text_snippets)
        return text_evidences, entities

    def wp_entity_retriever(self, entity):
        """
        Retrieve evidences from Wikipedia for the given Wikipedia title.
        Always returns the full set of evidences (text, table, infobox).
        Filtering is done via filter_evidences function.
        """
        # retrieve Wikipedia soup
        entity_id = entity["id"]

        if self.use_cache and entity_id in self.wikipedia_dump:
            self.logger.debug(f"Found Wikipedia evidences in dump!")
            evidences = self.wikipedia_dump.get(entity_id)

        else:
            # get Wikipedia title
            wiki_path = self.wikipedia_mappings.get(entity_id)
            if not wiki_path:
                # print(f"No Wikipedia link found for this Wikidata ID: {question_entity_id}.")
                self.logger.debug(
                    f"No Wikipedia link found for this Wikidata ID: {entity_id}."
                )
                if self.use_cache:
                    self.wikipedia_dump[entity_id] = []  # remember
                return []
            self.logger.debug(f"Retrieving Wikipedia evidences for: {wiki_path}.")
            self.dump_changed = True

            # retrieve Wikipedia soup
            wiki_title = _wiki_path_to_title(wiki_path)

            soup = self._retrieve_soup(wiki_title)
            if soup is None:
                if self.use_cache:
                    self.wikipedia_dump[entity_id] = []  # remember
                return []

            # retrieve Wikipedia markdown
            wiki_md = self._retrieve_markdown(wiki_title)

            # extract anchors
            doc_anchor_dict = self._build_document_anchor_dict(soup)

            # retrieve evidences
            infobox_evidences = self._retrieve_infobox_entries(wiki_title, soup, doc_anchor_dict)
            text_snippets = self._retrieve_text_snippets(wiki_title, wiki_md)

            for evidence in text_snippets:
                evidence["index"] = text_snippets.index(evidence)
                evidence["retrieved_for_entity"] = entity

            for evidence in infobox_evidences:
                evidence["index"] = infobox_evidences.index(evidence)
                evidence["retrieved_for_entity"] = entity

            evidences = infobox_evidences + text_snippets

            self.annotator.annotate_wikidata_entities(wiki_path, evidences, doc_anchor_dict)

            if self.use_cache and entity_id not in self.wikipedia_dump:
                self.wikipedia_dump[entity_id] = evidences
                self.dump_changed = True

        self.logger.debug(f"Evidences successfully retrieved for {entity}.")
        evidences = self.entity_evidences_selection(evidences)
        # evidences = self.filter_and_clean_evidences(evidences)
        self.logger.debug(f"Evidences successfully retrieved for {entity}.")
        return evidences

    def entity_evidences_selection(self, evidences):
        """
        Drop evidences which do not suffice specific
        criteria. E.g. such evidences could be too
        short, long, or contain too many symbols.
        """
        selected_evidences = list()
        for evidence in evidences:
            # only keep evidences having timespans
            non_date_entities = self._filter_noise_entity_evidence(evidence)
            if non_date_entities:
                if evidence not in selected_evidences:
                    # remove timestamps from wikidata_entities
                    evidence["wikidata_entities"] = non_date_entities
                    selected_evidences.append(evidence)
        return selected_evidences

    def _retrieve_infobox_entries(self, wiki_title, soup, doc_anchor_dict):
        """
        Retrieve infobox entries for the given Wikipedia entity.
        """
        # get infobox (only one infobox possible)

        infoboxes = soup.find_all("table", {"class": "infobox"})
        if not infoboxes:
            return []
        infobox = infoboxes[0]

        # parse infobox content
        p = InfoboxParser(doc_anchor_dict)
        infobox_html = str(infobox)
        p.feed(infobox_html)

        # transform parsed infobox to evidences
        infobox_parsed = p.tables[0]

        evidences = infobox_to_evidences(infobox_parsed, wiki_title)

        return evidences

    def _retrieve_text_snippets(self, wiki_title, wiki_md):
        """
        Retrieve text snippets for the given Wikidata entity.
        """
        evidences = extract_text_snippets(wiki_md, wiki_title, self.nlp)
        return evidences

    def _build_document_anchor_dict(self, soup):
        """
        Establishes a dictionary that maps from Wikipedia text
        to the Wikipedia entity (=link). Is used to map to
        Wikidata entities (via Wikipedia) later.
        Format: text -> Wikidata entity.
        """
        # prune navigation bar
        for div in soup.find_all("div", {"class": "navbox"}):
            div.decompose()

        # go through links
        anchor_dict = dict()
        for tag in soup.find_all("a"):
            # anchor text
            text = tag.text.strip()
            if len(text) < 3:
                continue
            # duplicate anchor text (keep first)
            # -> later ones can be more specific/incorrect
            if anchor_dict.get(text):
                continue

            # wiki title (=entity)
            href = tag.attrs.get("href")
            if not is_wikipedia_path(href):
                continue
            wiki_path = format_wiki_path(href)

            anchor_dict[text] = wiki_path
        return anchor_dict

    def safequote(self, string):
        """
        Try to UTF-8 encode and percent-quote string
        """
        if string is None:
            return
        try:
            return quote(string.encode("utf-8"))
        except UnicodeDecodeError:
            return quote(string)

    def parse(self, title, link):
        """
        Returns Mediawiki action=parse query string
        """
        endpoint = "/w/api.php"
        # API_URL
        qry = self.PARSE.substitute(
            WIKI="https://en.wikipedia.org", ENDPOINT=endpoint, PAGE=self.safequote(title)
        )

        return qry

    def _retrieve_soup(self, wiki_title):
        """
        Retrieve Wikipedia html for the given Wikipedia Title.
        """
        wiki_path = _wiki_title_to_path(wiki_title)
        link = f"https://en.wikipedia.org/wiki/{wiki_path}"
        try:
            page = requests.get(link)
            # Create your etree with a StringIO object which functions similarly
            # to a fileHandler
            # Decode the page content from bytes to string
            # page_utf8 = page.content.decode("utf-8")
            # ptree = etree.parse(StringIO(page_utf8), parser=parser)
            soup = BeautifulSoup(page.text, features="html.parser")
        except:
            return None
        return soup

    def _retrieve_markdown(self, wiki_title):
        """
        Retrieve the content of the given wikipedia title.
        """
        params = PARAMS.copy()
        params["titles"] = wiki_title
        try:
            # make request
            r = requests.get(API_URL, params=params)
            res = r.json()
            pages = res["query"]["pages"]
            page = list(pages.values())[0]
        except:
            return None
        return page

    def _init_wikipediaentity_dump(self):
        """
        Initialize the Wikipedia dump. The consists of a mapping
        from Wikidata IDs to Wikipedia evidences in the expected format.
        """
        if os.path.isfile(self.path_to_dump):
            # remember version read initially
            self.logger.info(f"Loading Wikipedia dump from path {self.path_to_dump}.")
            with FileLock(f"{self.path_to_dump}.lock"):
                self.dump_version = self._read_dump_version()
                self.logger.info(f"Dump version {self.dump_version}.")
                self.wikipedia_dump = self._read_dump()
            self.logger.info(f"Wikipedia dump successfully loaded.")
        else:
            self.logger.info(
                f"Could not find an existing Wikipedia dump at path {self.path_to_dump}."
            )
            self.logger.info("Populating Wikipedia dump from scratch!")
            self.wikipedia_dump = {}
            self._write_dump(self.wikipedia_dump)
            self._write_dump_version()

    def store_dump(self):
        """Store the Wikipedia dumo to disk."""
        if not self.use_cache:  # store only if Wikipedia dump in use
            return
        if not self.dump_changed:  # store only if Wikipedia dump  changed
            return
        # check if the Wikipedia dump  was updated by other processes
        if self._read_dump_version() == self.dump_version:
            # no updates: store and update version
            self.logger.info(f"Writing Wikipedia dump at path {self.path_to_dump}.")
            with FileLock(f"{self.path_to_dump}.lock"):
                self._write_dump(self.wikipedia_dump)
                self._write_dump_version()
        else:
            # update! read updated version and merge the dumps
            self.logger.info(f"Merging Wikipedia dump at path {self.path_to_dump}.")
            with FileLock(f"{self.path_to_dump}.lock"):
                # read updated version
                updated_dump = self._read_dump()
                # overwrite with changes in current process (most recent)
                updated_dump.update(self.wikipedia_dump)
                # store
                self._write_dump(updated_dump)
                self._write_dump_version()

    def _read_dump(self):
        """
        Read the current version of the dump.
        This can be different from the version used in this file,
        given that multiple processes may access it simultaneously.
        """
        # read file content from wikipedia dump shared across QU methods
        self.logger.info(f"Start read dump {self.path_to_dump}.")
        with open(self.path_to_dump, "rb") as fp:
            wikipedia_dump = pickle.load(fp)
        return wikipedia_dump

    def _write_dump(self, dump):
        """Store the dump."""
        dump_dir = os.path.dirname(self.path_to_dump)
        Path(dump_dir).mkdir(parents=True, exist_ok=True)
        with open(self.path_to_dump, "wb") as fp:
            pickle.dump(dump, fp)
        return dump

    def _read_dump_version(self):
        """Read the dump version (hashed timestamp of last update) from a dedicated file."""
        if not os.path.isfile(f"{self.path_to_dump}.version"):
            self._write_dump_version()
        with open(f"{self.path_to_dump}.version", "r") as fp:
            dump_version = fp.readline().strip()
        return dump_version

    def _write_dump_version(self):
        """Write the current dump version (hashed timestamp of current update)."""
        with open(f"{self.path_to_dump}.version", "w") as fp:
            version = str(time.time())
            fp.write(version)
        self.dump_version = version
