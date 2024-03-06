import json
import logging
import os
import pickle
import traceback

import requests

import tiq.library.wikipedia_library as wiki
from tiq.library.string_library import StringLibrary as string_lib
from tiq.library.utils import get_qid

MAX_WIKI_PATHS_PER_REQ = 50

# supress warnings on parser errors
logging.getLogger("wikitables").setLevel("ERROR")
MONTH_PREFIX = "Portal:Current_events"


class EvidenceAnnotator:
    """
    Annotate evidences with entities, dates and potentially other constants.
    """

    def __init__(self, config, temporal_expression, wikidata_mappings):
        self.config = config
        self.wikidata_mappings = wikidata_mappings
        self.date_annotation = temporal_expression.date_ordinal_annotator
        self.temporal_expression_ann = temporal_expression
        self.reference_time = self.config["reference_time"]
        self.date_tag_method = "regex"
        # load Wikidata labels
        with open(os.path.join(self.config["data_path"], self.config["path_to_labels"]), "rb") as fp:
            self.labels_dict = pickle.load(fp)

        # initialize cache
        self.path = os.path.join(self.config["data_path"], config["path_to_cache_wikipedia_to_wikidata"])
        self.label_not_in_dictionary = []
        self._init_cache()

    def annotate_wikidata_events(self, wiki_path, doc_anchor_dict):
        doc_anchor_tuples = [(key, value) for key, value in doc_anchor_dict.items()]
        doc_anchor_tuples = sorted(doc_anchor_tuples, key=lambda y: len(y[0]), reverse=True)

        wikipedia_paths = list()
        for anchor_text, anchor_path in doc_anchor_tuples:
            if "https" in anchor_path: continue
            if "Portal:Current_events" in anchor_path: continue
            if "/" in anchor_path: continue
            if "Special:" in anchor_path: continue
            if "#" in anchor_path: continue
            wikipedia_paths.append(anchor_path)

        redirects = self.extract_redirects(wikipedia_paths)

        # Wikipedia -> Wikidata
        wikidata_ids = list()
        for i, wiki_path in enumerate(wikipedia_paths):
            wikidata_id = self._wiki_path_to_wikidata(wiki_path, redirects)
            wikidata_ids.append(wikidata_id)

        # drop False values (wiki path could not be translated to wikidata)
        wikidata_ids = list(set([entity for entity in wikidata_ids if entity]))
        wikidata_entities = [{"id": item_id, "label": string_lib.item_to_label(item_id, self.labels_dict)}
                             for item_id in wikidata_ids]

        for item in wikidata_entities:
            if item["id"] == item["label"] and item["label"][0] == "Q":
                self.label_not_in_dictionary.append(item["id"])

        return wikidata_entities

    def annotate_wikidata_entities(self, wiki_path, evidences, doc_anchor_dict):
        """
        Add Wikidata entities, dates and potentially other constants to evidences.
        """
        # sort anchor-texts by their length
        doc_anchor_tuples = [(key, value) for key, value in doc_anchor_dict.items()]
        doc_anchor_tuples = sorted(doc_anchor_tuples, key=lambda y: len(y[0]), reverse=True)

        for evidence in evidences:
            # detect wikipedia entities
            if not evidence.get("source") == "info":  # entities for infobox are already done
                wiki_paths, disambiguations = self._detect_wikipedia_entities(
                    wiki_path, evidence, doc_anchor_tuples
                )
                evidence["wikipedia_paths"] = wiki_paths
                evidence["wp_disambiguations"] = disambiguations

            # add page title upfront (for additional context), and corresponding disambiguations
            page_entity_id = evidence["retrieved_for_entity"]["id"]
            wiki_title = wiki._wiki_path_to_title(wiki_path)
            # improve evidence_text
            evidence_text = evidence["evidence_text"]
            evidence_text = evidence_text.replace("\n", " ").replace("\t", " ")
            while "  " in evidence_text:
                evidence_text = evidence_text.replace("  ", " ")
            evidence_text = evidence_text.strip()
            evidence["evidence_text"] = f'{wiki_title}, {evidence_text}'
            # date_annotate_evidences.append([evidence["evidence_text"], self.reference_time])
            evidence["wikidata_ids"] = [page_entity_id]
            evidence["disambiguations"] = [(wiki_title, page_entity_id)]

            annotation_result, explicit_expression, date_annotator_result, _ = self.temporal_expression_ann.annotateExplicitTemporalExpressions(
                evidence["evidence_text"], self.config["reference_time"], self.date_tag_method)

            evidence["explicit_expression"] = explicit_expression

            dates = list()
            timetexts = list()
            disambiguations = list()
            timespan = list()
            position = list()
            for result in date_annotator_result:
                timespan.append(result['timespan'])
                timetexts.append(result['text'])
                dates += [item[0] for item in result['disambiguation']]
                disambiguations += result['disambiguation']
                position.append(result['span'])

            timestamp_ids = [item[1] for item in disambiguations if len(item) == 2]
            evidence["wikidata_ids"] += timestamp_ids
            evidence["disambiguations"] += disambiguations
            evidence["tempinfo"] = [timespan, disambiguations, dates, timetexts,
                                    position] if timespan and disambiguations else None

            if len(evidence["wikipedia_paths"]) + len(evidence["wikidata_ids"]) <= 1:
                continue

        # retrieve redirects
        all_wiki_paths = [
            wiki_path for evidence in evidences for wiki_path in evidence["wikipedia_paths"]
        ]
        redirects = self.extract_redirects(all_wiki_paths)

        # Wikipedia -> Wikidata
        for evidence in evidences:
            wiki_paths = evidence["wikipedia_paths"]
            disambiguations = evidence["wp_disambiguations"]
            # transformation
            wikidata_ids = list()
            for i, wiki_path in enumerate(wiki_paths):
                wikidata_id = self._wiki_path_to_wikidata(wiki_path, redirects)
                wikidata_ids.append(wikidata_id)
                dis_text, _ = disambiguations[i]
                disambiguations[i] = (dis_text, wikidata_id)

            # drop False values (wiki path could not be translated to wikidata)
            wikidata_ids = [entity for entity in wikidata_ids if entity]
            evidence["wikidata_ids"] += wikidata_ids
            evidence["disambiguations"] += disambiguations

            # drop duplicates
            evidence["wikidata_ids"] = list(set(evidence["wikidata_ids"]))

            # add labels to obtain Wikidata entities
            evidence["wikidata_entities"] = [
                {"id": item_id, "label": string_lib.item_to_label(item_id, self.labels_dict)}
                for item_id in evidence["wikidata_ids"]
            ]
            for item in evidence["wikidata_entities"]:
                if item["id"] == item["label"] and item["label"][0] == "Q":
                    self.label_not_in_dictionary.append(item["id"])

            del evidence["wikidata_ids"]
            del evidence["wikipedia_paths"]
            del evidence["wp_disambiguations"]

    def _detect_wikipedia_entities(self, wiki_path, evidence, doc_anchor_tuples):
        """
        Identify Wikipedia entities in the given evidence using
        the given anchor dict for the Wikipedia page.
        Longer matches would be checked first.
        """
        # remember all anchor texts for prunings
        finds = list()
        disambiguations = list()
        evidence_text = evidence["evidence_text"]

        wikipedia_paths = list()
        for anchor_text, anchor_path in doc_anchor_tuples:
            if anchor_text in evidence_text:
                ## do not consider wiki_paths with hashtags
                # hashtag indicates a paragraph on entity, rather than entity
                if "#" in anchor_path:
                    continue

                # search start and end points
                new_start = evidence_text.find(anchor_text)
                new_end = new_start + len(anchor_text)

                ## detect duplicate match for substring
                # positions must be inside range of [_start,_end]
                # since anchor texts are sorted by length
                duplicate = False
                for _start, _end in finds:
                    if new_start >= _start and new_start <= _end:
                        duplicate = True
                    elif new_end >= _start and new_end <= _end:
                        duplicate = True

                # if no duplicate match found -> new anchor
                if not duplicate:
                    finds.append((new_start, new_end))
                    wikipedia_paths.append(anchor_path)
                    disambiguations.append((anchor_text, anchor_path))

        # add path of Wikipedia page entity
        if not wiki_path in wikipedia_paths:
            wikipedia_paths.append(wiki_path)
            wiki_title = wiki._wiki_path_to_title(wiki_path)
            disambiguations.append((wiki_title, wiki_path))

        return wikipedia_paths, disambiguations

    def _extract_dates_multithread(self, evidence_texts):
        """
        Extract dates in texts with multithread (added to entities).
        First, text is searched for text, then the dates
        are brought into a compatible format (timestamps).
        """
        # dates in standard Wikipedia formats

        evidence_texts_dates = list()

        date_annotation_results = self.date_annotation.date_annotator_multithread(evidence_texts, self.date_tag_method)
        # # detect dates, ordinals, and signals
        for evidence_result in date_annotation_results:
            dates = list()
            disambiguations = list()
            timespan = list()
            texts = list()
            span = list()
            for result in evidence_result:
                timespan.append(result['timespan'])
                dates += [item[0] for item in result['disambiguation']]
                disambiguations += result['disambiguation']
                texts.append(result['text'])
                span.append(result['span'])
            evidence_texts_dates.append([dates, timespan, disambiguations, texts, span])
        return evidence_texts_dates

    def _extract_dates(self, evidence_text):
        """
        Extract dates in text (added to entities).
        First, text is searched for text, then the dates
        are brought into a compatible format (timestamps).
        """
        # dates in standard Wikipedia formats
        dates = list()
        disambiguations = list()
        # replaced by date annotation module
        annotation_results = self.date_annotation.date_annotator(evidence_text)
        for result in annotation_results:
            dates += [item[0] for item in result['disambiguation']]
            disambiguations += result['disambiguation']

        return dates, disambiguations

    def _get_qid_on_the_fly(self, wiki_path):
        wikipedia_link = f"https://en.wikipedia.org/wiki/{wiki_path}"
        result = get_qid(wikipedia_link)
        if result:
            return result[0]
        else:
            return None

    def _wiki_path_to_wikidata(self, wiki_path, redirects):
        """
        Transform the given Wikipedia path to the Wikidata ID.
        """
        if wiki_path in self.cache:
            return self.cache[wiki_path]

        # try look-up
        if self.wikidata_mappings.get(wiki_path):
            wikidata_id = self.wikidata_mappings.get(wiki_path)
        elif self.wikidata_mappings.get(wiki_path.replace(".", "")):
            wikidata_id = self.wikidata_mappings.get(wiki_path.replace(".", ""))
        # try via redirect look-up
        elif redirects.get(wiki_path):
            wiki_title = redirects.get(wiki_path)
            wiki_path = wiki._wiki_title_to_path(wiki_title)
            # try via dict
            if self.wikidata_mappings.get(wiki_path):
                wikidata_id = self.wikidata_mappings.get(wiki_path)
                # -> different from None return value
            else:
                # self.qid_not_in_dictionary.append(wiki_path)
                return None
        else:
            # self.qid_not_in_dictionary.append(wiki_path)
            return None

        self.cache[wiki_path] = wikidata_id
        return wikidata_id

    def extract_redirects(self, wiki_paths):
        """
        Extract redirects for set of Wikipedia paths (one entity can have multiple paths).
        Makes use of _extract_redirects_for_50 function to decrease number
        of requests required.
        """

        wiki_paths = list(set(wiki_paths))

        # drop wiki_paths for which Wikidata mapping is already known (redirect not required)
        wiki_paths = [
            wiki_path
            for wiki_path in wiki_paths
            if wiki_path and self._wiki_path_to_wikidata(wiki_path, {}) is None
        ]

        # limit for wiki_paths per request is 50
        start_index = 0
        redirects = dict()
        while start_index < len(wiki_paths):
            end_index = min(start_index + MAX_WIKI_PATHS_PER_REQ, len(wiki_paths) - 1)
            wiki_paths_batch = wiki_paths[start_index:end_index]
            new_redirects = self._extract_redirects_for_50(wiki_paths_batch)
            redirects.update(new_redirects)
            start_index += MAX_WIKI_PATHS_PER_REQ
        return redirects

    def _extract_redirects_for_50(self, wiki_paths):
        """
        Extract redirects of given (max.) 50 Wikipedia paths (one entity can have multiple paths).
        Used in extract_redirects function for efficiency.
        """
        if not wiki_paths:
            return {}

        # initialize
        redirects = dict()

        try:
            # create request url
            wiki_paths_string = "|".join(wiki_paths)
            url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&titles={wiki_paths_string}&redirects"

            # retrieve result
            res = requests.get(url)
            res_dict = json.loads(res.content)

            ## result has mappings:
            #   normalized: wiki_path -> wiki_title
            #   redirects: wiki_title -> redirected wiki_title
            if res_dict["query"].get("normalized"):
                normalized = {
                    normalized["to"]: normalized["from"]
                    for normalized in res_dict["query"]["normalized"]
                }
            else:
                normalized = dict()

            # if redirects not set, no redirects required!
            if not res_dict["query"].get("redirects"):
                return redirects

            # create redirects dict
            for redirect in res_dict["query"]["redirects"]:
                # get key
                if normalized.get(redirect["from"]):
                    key = normalized[redirect["from"]]
                else:
                    key = redirect["from"]

                # add entry
                redirects[key] = redirect["to"]

        # catch exception and log problem
        except Exception as e:
            print(f"Error catched for url: {url}")
            print(e)
            if hasattr(e, "__traceback__"):
                traceback.print_tb(e.__traceback__)

        return redirects

    def store_cache(self):
        """Store the cache to disk."""
        with open(self.path, "wb") as fp:
            pickle.dump(self.cache, fp)

    def _init_cache(self):
        """Initialize the cache."""
        if os.path.isfile(self.path):
            with open(self.path, "rb") as fp:
                self.cache = pickle.load(fp)
        else:
            self.cache = dict()
