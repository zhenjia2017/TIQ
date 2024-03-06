import os
import pickle
import re
import time
from pathlib import Path

from filelock import FileLock

import tiq.library.wikipedia_library as wiki
from tiq.library.utils import get_logger, format_text

ENT_PATTERN = re.compile("^Q[0-9]+$")
PRE_PATTERN = re.compile("^P[0-9]+$")
KB_ITEM_SEPARATOR = ", "

API_URL = "http://en.wikipedia.org/w/api.php"
PARAMS = {
    "prop": "extracts|revisions",
    "format": "json",
    "action": "query",
    "explaintext": "",
    "rvprop": "content",
}


class InformationRetriever:
    """
    Variant of the Wikipedia retrieval for entities.
    """

    def __init__(self, config, wp_retriever, year_start, year_end):

        self.config = config
        self.logger = get_logger(__name__, config)
        self.wp_retriever = wp_retriever
        self.clocq = self.wp_retriever.clocq

        self.year_start = year_start
        self.year_end = year_end
        self.reference_end_time = self.config["reference_end_time"] + "T00:00:00Z"
        self.data_path = self.config["data_path"]
        self.use_cache = True
        self.data_path = self.config["data_path"]
        self.temporal_fact_dump_file = self.config["temporal_fact_dump_file"]
        self.source = self.config["source"]
        # cache path
        self.path_to_dump = os.path.join(self.data_path, self.temporal_fact_dump_file)

        if self.use_cache:
            # initialize cache
            self._init_information_snippet_dump()
            self.dump_changed = False

        self.entity_type_map = {}

    def retrieve_info_wikidata(self, entity):
        qualifier_temporal_evidences, main_temporal_facts = self.retrieve_kb_facts(entity)
        evidences = qualifier_temporal_evidences + main_temporal_facts
        for evidence in evidences:
            self.add_type_to_entity(evidence)
            answer_entity = []
            for item in evidence["answer_entity"]:
                # drop the entities without type or type is Wikimedia list article
                if item["type"] == "NULL" or item["type"] == "Wikimedia list article":
                    continue
                answer_entity.append(item)
            evidence["answer_entity"] = answer_entity
        return evidences

    def retrieve_info_wikipedia(self, entity):
        evidences = []
        wiki_evidences = self.wp_retriever.wp_entity_retriever(entity)

        for evidence in wiki_evidences:
            # print (evidence)
            self.wiki_evidence_to_template(evidence)
            evidences.append(evidence)
        return evidences

    def retrieve_evidences_from_heterogeneous_sources(self, entity):
        """
        Retrieve temporal facts from kg and texts and infoboxes from wikipedia
        """
        evidences = []
        if "kb" in self.config["source"]:
            evidences += self.retrieve_info_wikidata(entity)
        if "text" in self.config["source"] or "info" in self.config["source"]:
            wikipedis_evidences = self.retrieve_info_wikipedia(entity)
            for evidence in wikipedis_evidences:
                if "text" in self.config["source"] and evidence["source"] == "text":
                    evidences.append(evidence)
                if "info" in self.config["source"] and evidence["source"] == "info":
                    evidences.append(evidence)

        self.logger.debug(f"Number of evidences : {len(evidences)}")

        return evidences

    def remove_noise_char_from_text(self, text):
        tokens = text.split()
        if len(tokens) > 0:
            wiki_path = tokens[0]
            wiki_title = wiki._wiki_path_to_title(wiki_path)
            tokens[0] = wiki_title
            text = ' '.join(tokens)
        tokens = text.split()
        tokens_remove_blank = []
        for token in tokens:
            tokens_remove_blank.append(token.strip())
        text = ' '.join(tokens_remove_blank)
        if text.endswith(','):
            text = text[:-1]
        if text.endswith('.'):
            text = text[:-1]
        return text

    def clean_text(self, text):
        # remove ": " from the text
        text = re.sub(r"\[[0-9]*\]", "", text)
        new_text = text.replace(": ", " ")
        # remove non-alpha charactor from the beginning of the text
        text_position = []
        for char in new_text:
            text_position.append(new_text.index(char))
            if char.isalpha():
                break
        new_text = new_text[:text_position[0]] + new_text[text_position[-1]:]
        cleaned_text = self.remove_noise_char_from_text(new_text)
        cleaned_text = cleaned_text.encode('utf-8').decode('utf-8')
        cleaned_text = format_text(cleaned_text)
        cleaned_text = f"{cleaned_text}."
        return cleaned_text

    def wiki_evidence_to_template(self, evidence):
        explicit_expression = evidence["explicit_expression"]
        # we only keep the texts with overlap and duration signals
        text = evidence['evidence_text']
        # remove date expression from text
        if len(explicit_expression) == 1:
            start_pos = int(explicit_expression[0]["span"][0])
            end_pos = int(explicit_expression[0]["span"][1])
            text = text[:start_pos] + text[end_pos:]
        elif len(explicit_expression) > 1:
            date_positions_in_text = [(int(item["span"][0]), int(item["span"][1])) for item in explicit_expression]
            # there are duplicated expressions with same dates
            # remove dates from back to front
            sorted_data = sorted(date_positions_in_text, key=lambda x: list(x)[1], reverse=True)
            for item in sorted_data:
                start_pos = item[0]
                end_pos = item[1]
                text = text[:start_pos] + text[end_pos:]

        cleaned_text = self.clean_text(text)
        evidence["candidate_question_text"] = cleaned_text
        disambiguations = {item[1]: item[0] for item in evidence["disambiguations"]}
        for item in evidence["wikidata_entities"]:
            item.update({"disam_label": disambiguations[item["id"]]})

        evidence["answer_entity"] = [item for item in evidence["wikidata_entities"] if (
                item["id"] != evidence["retrieved_for_entity"]["id"] and item["type"] != "NULL" and item[
            "type"] != "Wikimedia list article")]

    def add_type_to_entity(self, evidence):
        for item in evidence["wikidata_entities"]:
            if item["id"] in self.entity_type_map:
                type = self.entity_type_map[item["id"]]
                if type:
                    item["type"] = type["label"]
                else:
                    item["type"] = "NULL"
            else:
                type = self.clocq.get_type(item["id"])
                self.entity_type_map[item["id"]] = type
                if type:
                    item["type"] = type["label"]
                else:
                    item["type"] = "NULL"

    def retrieve_kb_facts(self, entity):
        """Retrieve evidences from KB for the given item (used in DS)."""
        entity_id = entity["id"]
        if self.use_cache and entity_id in self.information_dump:
            self.logger.debug(f"Found Information snippets in dump!")
            facts = self.information_dump.get(entity_id)
        else:
            facts = self.clocq.get_neighborhood(entity_id, p=self.config["clocq_p"], include_labels=True)
            if self.use_cache and entity_id not in self.information_dump:
                self.information_dump[entity_id] = facts
                self.dump_changed = True

        self.logger.debug(f"Number of facts : {len(facts)}")
        temporal_facts = []
        for fact in facts:
            iff_temporal = self._iff_temporal_fact(fact)
            if iff_temporal:
                for item in fact:
                    item["id"] = item["id"].replace('"', '')
                    item['label'] = item['label'].replace('"', '')
                temporal_facts.append(fact)
        self.logger.debug(f"Number of temporal facts : {len(temporal_facts)}")
        evidences = self._kb_fact_to_event(temporal_facts, entity)

        return evidences

    def _kb_fact_to_event(self, kb_facts, entity):
        """Transform the given KB-fact to an evidence."""

        def _remove_time_from_fact(fact):
            candidate_question_text = ''
            # remove time from evidence text
            main_fact = fact[0:3].copy()
            for item in main_fact:
                if "T00:00:00Z" in item["id"]:
                    time_label = main_fact[main_fact.index(item) - 1]["label"]
                    candidate_question_text = candidate_question_text.rstrip(time_label)
                    continue
                candidate_question_text += " " + item["label"]

            pqn = int((len(fact) - 3) / 2)
            if pqn > 0:
                for i in range(pqn):
                    # (predicate, date)
                    pq_fact = (fact[3 + i * 2], fact[4 + i * 2])
                    if "T00:00:00Z" in pq_fact[1]["id"]:
                        continue
                    candidate_question_text += " " + pq_fact[0]["label"] + " " + pq_fact[1]["label"]

            candidate_question_text = candidate_question_text.strip()

            return candidate_question_text

        def _extract_qualifier_start_end_fact(fact, entity):
            start_time = None
            end_time = None
            timespan = []
            disambiguation = []
            evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact])
            if len(fact) > 5:
                # only keep the fact with the item from 1 to 5
                candidate_question_text = _remove_time_from_fact(fact[0:5])
                answer_entity = [item for item in fact[0:5] if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]
            else:
                candidate_question_text = _remove_time_from_fact(fact)
                answer_entity = [item for item in fact if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]

            for fitem in fact:
                index = fact.index(fitem)
                if 'P580' in fitem['id']:
                    item_af = fact[index + 1]
                    start_time = item_af['id']
                    start_time_label = item_af['label']
                    if (start_time_label, start_time) not in disambiguation:
                        disambiguation.append((start_time_label, start_time))
                if 'P582' in fitem['id']:
                    item_af = fact[index + 1]
                    end_time = item_af['id']
                    end_time_label = item_af['label']
                    if (end_time_label, end_time) not in disambiguation:
                        disambiguation.append((end_time_label, end_time))
                    if "-01-01T00:00:00Z" in end_time:
                        # for year we extend it to the end of the year
                        end_time = end_time.replace("-01-01T00:00:00Z", "-12-31T00:00:00Z")

            if start_time and end_time:
                timespan.append([start_time, end_time])
            elif start_time and not end_time:
                timespan.append([start_time, self.reference_end_time])
            else:
                return None

            wikidata_entities = [item for item in fact if item["id"][0] == "Q"]
            evidence = {"evidence_text": evidence_text, "relation": fact[1], "source": "kb",
                        "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                        "candidate_question_text": candidate_question_text, "wikidata_entities": wikidata_entities,
                        "fact": fact, "answer_entity": answer_entity, "main_fact": fact[0]["id"] == entity["id"]}
            return evidence

        def _extract_qualifier_point_in_fact(fact, entity):
            start_time = None
            end_time = None
            timespan = []
            disambiguation = []
            evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact])
            if len(fact) > 5:
                # only keep the fact with the item from 1 to 5
                candidate_question_text = _remove_time_from_fact(fact[0:5])
                answer_entity = [item for item in fact[0:5] if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]
            else:
                candidate_question_text = _remove_time_from_fact(fact)
                answer_entity = [item for item in fact if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]

            for fitem in fact:
                index = fact.index(fitem)
                if 'P585' in fitem['id']:
                    item_af = fact[index + 1]
                    start_time = item_af['id']
                    start_time_label = item_af['label']
                    if (start_time_label, start_time) not in disambiguation:
                        disambiguation.append((start_time_label, start_time))
                    end_time = item_af['id']
                    end_time_label = item_af['label']
                    if (end_time_label, end_time) not in disambiguation:
                        disambiguation.append((end_time_label, end_time))
                    if "-01-01T00:00:00Z" in end_time:
                        # for year we extend it to the end of the year
                        end_time = end_time.replace("-01-01T00:00:00Z", "-12-31T00:00:00Z")
            if start_time and end_time:
                timespan.append([start_time, end_time])
                wikidata_entities = [item for item in fact if item["id"][0] == "Q"]
                evidence = {"evidence_text": evidence_text, "relation": fact[1], "source": "kb",
                            "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                            "candidate_question_text": candidate_question_text, "wikidata_entities": wikidata_entities,
                            "fact": fact, "answer_entity": answer_entity, "main_fact": fact[0]["id"] == entity["id"]}
                return evidence

        def _extract_qualifier_publication_fact(fact, entity):
            start_time = None
            end_time = None
            timespan = []
            disambiguation = []
            evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact])
            if len(fact) > 5:
                # only keep the fact with the item from 1 to 5
                # evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact[0:5]])
                candidate_question_text = _remove_time_from_fact(fact[0:5])
                answer_entity = [item for item in fact[0:5] if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]
            else:
                # evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact])
                candidate_question_text = _remove_time_from_fact(fact)
                answer_entity = [item for item in fact if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]

            for fitem in fact:
                index = fact.index(fitem)
                if 'P577' in fitem['id']:
                    item_af = fact[index + 1]
                    start_time = item_af['id']
                    start_time_label = item_af['label']
                    if (start_time_label, start_time) not in disambiguation:
                        disambiguation.append((start_time_label, start_time))
                    end_time = item_af['id']
                    end_time_label = item_af['label']
                    if (end_time_label, end_time) not in disambiguation:
                        disambiguation.append((end_time_label, end_time))
                    if "-01-01T00:00:00Z" in end_time:
                        # for year we extend it to the end of the year
                        end_time = end_time.replace("-01-01T00:00:00Z", "-12-31T00:00:00Z")

            if start_time and end_time:
                timespan.append([start_time, end_time])
                wikidata_entities = [item for item in fact if item["id"][0] == "Q"]
                evidence = {"evidence_text": evidence_text, "relation": fact[1], "source": "kb",
                            "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                            "candidate_question_text": candidate_question_text, "wikidata_entities": wikidata_entities,
                            "fact": fact, "answer_entity": answer_entity, "main_fact": fact[0]["id"] == entity["id"]}
                return evidence

        def _extract_qualifier_unknown_fact(fact, entity):
            start_time = None
            end_time = None
            timespan = []
            disambiguation = []
            evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact])
            if len(fact) > 5:
                # only keep the fact with the item from 1 to 5
                # evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact[0:5]])
                candidate_question_text = _remove_time_from_fact(fact[0:5])
                answer_entity = [item for item in fact[0:5] if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]
            else:
                # evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact])
                candidate_question_text = _remove_time_from_fact(fact)
                answer_entity = [item for item in fact if
                                 (ENT_PATTERN.match(item["id"]) and item["id"] != entity["id"])]
            for fitem in fact:
                if "T00:00:00Z" in fitem['id']:
                    start_time = fitem['id']
                    start_time_label = fitem['label']
                    if (start_time_label, start_time) not in disambiguation:
                        disambiguation.append((start_time_label, start_time))
                    end_time = fitem['id']
                    end_time_label = fitem['label']
                    if (end_time_label, end_time) not in disambiguation:
                        disambiguation.append((end_time_label, end_time))
                    if "-01-01T00:00:00Z" in end_time:
                        # for year we extend it to the end of the year
                        end_time = end_time.replace("-01-01T00:00:00Z", "-12-31T00:00:00Z")
            if start_time and end_time:
                timespan.append([start_time, end_time])
                wikidata_entities = [item for item in fact if item["id"][0] == "Q"]
                evidence = {"evidence_text": evidence_text, "relation": fact[1], "source": "kb",
                            "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                            "candidate_question_text": candidate_question_text, "wikidata_entities": wikidata_entities,
                            "fact": fact, "answer_entity": answer_entity, "main_fact": fact[0]["id"] == entity["id"]}
                return evidence

        def _extract_main_start_end_fact(triple_kb_facts, entity):
            # there is event with start and end time
            start_time = None
            end_time = None
            timespan = []
            disambiguation = []
            start_fact = []
            end_fact = []
            for fact in triple_kb_facts:
                if fact[1]["id"] == "P580":
                    start_time = fact[2]["id"]
                    start_time_label = fact[2]['label']
                    if (start_time_label, start_time) not in disambiguation:
                        disambiguation.append((start_time_label, start_time))
                    start_fact = fact
                if fact[1]["id"] == "P582":
                    end_time = fact[2]["id"]
                    end_time_label = fact[2]['label']
                    if (end_time_label, end_time) not in disambiguation:
                        disambiguation.append((end_time_label, end_time))
                    if "-01-01T00:00:00Z" in end_time:
                        end_time = end_time.replace("-01-01T00:00:00Z", "-12-31T00:00:00Z")
                    end_fact = fact

            if start_time and end_time:
                timespan.append([start_time, end_time])
                evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in start_fact])
                evidence_text += KB_ITEM_SEPARATOR + KB_ITEM_SEPARATOR.join([item["label"] for item in end_fact[1:]])
                candidate_question_text = entity["label"] + ' ' + start_fact[1]["label"] + ' and ' + end_fact[1][
                    "label"]
                relation = [start_fact[1], end_fact[1]]
                evidence = {"evidence_text": evidence_text, "relation": relation, "source": "kb",
                            "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                            "candidate_question_text": candidate_question_text, "wikidata_entities": [entity],
                            "fact": [start_fact, end_fact], "answer_entity": []}
                return evidence

            elif start_time and not end_time:
                timespan.append([start_time, self.reference_end_time])
                evidence_text = ", ".join([item["label"] for item in start_fact])
                candidate_question_text = entity["label"] + ' ' + start_fact[1]["label"]
                relation = [start_fact[1]]
                evidence = {"evidence_text": evidence_text, "relation": relation, "source": "kb",
                            "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                            "candidate_question_text": candidate_question_text, "wikidata_entities": [entity],
                            "fact": [start_fact], "answer_entity": []}
                return evidence

        def _extract_main_temporal_fact(triple_kb_facts, entity):
            # there is event with start and end time
            evidences = []
            for fact in triple_kb_facts:
                start_time = None
                end_time = None
                timespan = []
                disambiguation = []
                evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in fact])
                candidate_question_text = " ".join([item["label"] for item in fact[0:2]])
                if "T00:00:00Z" in fact[2]["id"]:
                    start_time = fact[2]["id"]
                    start_time_label = fact[2]['label']
                    if (start_time_label, start_time) not in disambiguation:
                        disambiguation.append((start_time_label, start_time))
                    end_time = fact[2]["id"]
                    end_time_label = fact[2]['label']
                    if (end_time_label, end_time) not in disambiguation:
                        disambiguation.append((end_time_label, end_time))
                    if "-01-01T00:00:00Z" in end_time:
                        end_time = end_time.replace("-01-01T00:00:00Z", "-12-31T00:00:00Z")

                if start_time and end_time:
                    timespan.append([start_time, end_time])
                    evidence = {"evidence_text": evidence_text, "relation": fact[1], "source": "kb",
                                "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                                "candidate_question_text": candidate_question_text, "wikidata_entities": [entity],
                                "fact": fact, "answer_entity": []}
                    evidences.append(evidence)

                elif start_time and not end_time:
                    timespan.append([start_time, self.reference_end_time])
                    evidence = {"evidence_text": evidence_text, "relation": fact[1], "source": "kb",
                                "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                                "candidate_question_text": candidate_question_text, "wikidata_entities": [entity],
                                "fact": fact, "answer_entity": []}
                    evidences.append(evidence)

            return evidences

        def _extract_main_inception_dissolved_fact(triple_kb_facts, entity):
            # there is event with inception and dissolved time
            start_time = None
            end_time = None
            timespan = []
            disambiguation = []
            start_fact = []
            end_fact = []
            for fact in triple_kb_facts:
                if fact[1]["id"] == "P571":
                    start_time = fact[2]["id"]
                    start_time_label = fact[2]['label']
                    if (start_time_label, start_time) not in disambiguation:
                        disambiguation.append((start_time_label, start_time))
                    start_fact = fact
                if fact[1]["id"] == "P576":
                    end_time = fact[2]["id"]
                    end_time_label = fact[2]['label']
                    if (end_time_label, end_time) not in disambiguation:
                        disambiguation.append((end_time_label, end_time))
                    if "-01-01T00:00:00Z" in end_time:
                        end_time = end_time.replace("-01-01T00:00:00Z", "-12-31T00:00:00Z")
                    end_fact = fact
            if start_time and end_time:
                timespan.append([start_time, end_time])
                evidence_text = KB_ITEM_SEPARATOR.join([item["label"] for item in start_fact])
                evidence_text += KB_ITEM_SEPARATOR + KB_ITEM_SEPARATOR.join([item["label"] for item in end_fact[1:]])
                candidate_question_text = entity["label"] + ' ' + start_fact[1]["label"] + ' and ' + end_fact[1][
                    "label"]

                relation = [start_fact[1], end_fact[1]]
                evidence = {"evidence_text": evidence_text, "relation": relation, "source": "kb",
                            "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                            "candidate_question_text": candidate_question_text, "wikidata_entities": [entity],
                            "fact": [start_fact, end_fact], "answer_entity": []}
                return evidence

            elif start_time and not end_time:
                timespan.append([start_time, self.reference_end_time])
                evidence_text = ", ".join([item["label"] for item in start_fact])
                candidate_question_text = entity["label"] + ' ' + start_fact[1]["label"]
                relation = [start_fact[1]]
                evidence = {"evidence_text": evidence_text, "relation": relation, "source": "kb",
                            "retrieved_for_entity": entity, "tempinfo": [timespan, disambiguation],
                            "candidate_question_text": candidate_question_text, "wikidata_entities": [entity],
                            "fact": [start_fact], "answer_entity": []}
                return evidence

        def _format_fact(kb_facts, entity):
            """Correct format of fact (if necessary)."""
            triple_kb_facts = []
            qualifier_temporal_evidences = []
            main_temporal_evidences = []
            for kb_fact in kb_facts:
                if len(kb_fact) > 3:
                    # qualifier facts
                    relations = [item["id"] for item in kb_fact]
                    if 'P580' in relations:
                        qualifier_temporal_fact = _extract_qualifier_start_end_fact(kb_fact, entity)
                    elif 'P585' in relations:
                        qualifier_temporal_fact = _extract_qualifier_point_in_fact(kb_fact, entity)
                    elif 'P577' in relations:
                        qualifier_temporal_fact = _extract_qualifier_publication_fact(kb_fact, entity)
                    else:
                        qualifier_temporal_fact = _extract_qualifier_unknown_fact(kb_fact, entity)

                    if qualifier_temporal_fact:
                        qualifier_temporal_evidences.append(qualifier_temporal_fact)
                else:
                    triple_kb_facts.append(kb_fact)

            if triple_kb_facts:
                relations = [kb_fact[1]["id"] for kb_fact in triple_kb_facts]
                if "P580" in relations:
                    main_temporal_fact = _extract_main_start_end_fact(triple_kb_facts, entity)
                    if main_temporal_fact:
                        main_temporal_evidences.append(main_temporal_fact)
                if "P571" in relations:
                    main_temporal_fact = _extract_main_inception_dissolved_fact(triple_kb_facts, entity)
                    if main_temporal_fact:
                        main_temporal_evidences.append(main_temporal_fact)
                else:
                    main_temporal_facts = _extract_main_temporal_fact(triple_kb_facts, entity)
                    main_temporal_evidences += main_temporal_facts

            return qualifier_temporal_evidences, main_temporal_evidences

        qualifier_temporal_evidences, main_temporal_facts = _format_fact(kb_facts, entity)
        return qualifier_temporal_evidences, main_temporal_facts

    def _iff_temporal_fact(self, fact):
        for item in fact:
            if "T00:00:00Z" in item["id"]:
                return True
        return False

    def _init_information_snippet_dump(self):
        """
        Initialize the Wikipedia dump. The consists of a mapping
        from Wikidata IDs to Wikipedia evidences in the expected format.
        """
        if os.path.isfile(self.path_to_dump):
            # remember version read initially
            self.logger.info(f"Loading KB Information snippet dump from path {self.path_to_dump}.")
            with FileLock(f"{self.path_to_dump}.lock"):
                self.dump_version = self._read_dump_version()
                self.logger.info(f"Dump version {self.dump_version}.")
                self.information_dump = self._read_dump()
            self.logger.info(f"KB Information snippet successfully loaded.")
        else:
            self.logger.info(
                f"Could not find an existing KB Information snippet at path {self.path_to_dump}."
            )
            self.logger.info("Populating KB Information snippet from scratch!")
            self.information_dump = {}
            self._write_dump(self.information_dump)
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
            self.logger.info(f"Writing KB Information snippet at path {self.path_to_dump}.")
            with FileLock(f"{self.path_to_dump}.lock"):
                self._write_dump(self.information_dump)
                self._write_dump_version()
        else:
            # update! read updated version and merge the dumps
            self.logger.info(f"Merging KB Information snippet at path {self.path_to_dump}.")
            with FileLock(f"{self.path_to_dump}.lock"):
                # read updated version
                updated_dump = self._read_dump()
                # overwrite with changes in current process (most recent)
                updated_dump.update(self.information_dump)
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
        self.logger.info(f"Start read KB Information snippet dump {self.path_to_dump}.")
        with open(self.path_to_dump, "rb") as fp:
            information_dump = pickle.load(fp)
        return information_dump

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
