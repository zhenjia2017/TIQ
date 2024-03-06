# construct main and constraint candidates
# input: information snippets of sampled topic entities, information snippets of year pages
# output: main part candidates, constraint part candidates, and sequential main questions
import json
import string

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm

from tiq.library.utils import get_logger

nltk.download('punkt')  # Download the punkt tokenizer if not already downloaded
nltk.download('stopwords')  # Download the stopwords if not already downloaded

KB_ITEM_SEPARATOR = ", "


def remove_punctuation(text):
    clean_text = text.replace("( )", " ")
    clean_text = clean_text.replace("( % )", " ")
    clean_text = clean_text.replace(", ", " ")
    clean_text = clean_text.replace("•", " ")
    if len(clean_text) == 0: return clean_text
    if clean_text[-1] in string.punctuation:
        return clean_text[:-1].rstrip()
    else:
        return clean_text.rstrip()


def remove_all_punctuation(text):
    translator = str.maketrans("", "", string.punctuation)
    clean_text = text.translate(translator)
    return clean_text


def remove_multispace(text):
    while "  " in text:
        text = text.replace("  ", " ")
    return text


class MainConstraintGeneration:
    def __init__(self, config, clocq):
        # load or generate frequency for each qid
        self.config = config
        self.logger = get_logger(__name__, config)

        self.clocq = clocq
        # Load a pre-trained model
        self.model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
        self.similar_threshold = self.config["similar_threshold"]
        self.text_similar_threshold = self.config["text_similar_threshold"]
        self.max_date = int(self.config["MAX_DATE"].replace("-", ""))
        self.min_date = int(self.config["MIN_DATE"].replace("-", ""))
        self.entity_type_map = {}

    def main_constraint_generation(self, entity_evidence_file, year_page_path, iterative_number):
        count = 1
        evidences = []
        with open(entity_evidence_file, "r") as fp:
            for line in tqdm(fp):
                try:
                    evidence = json.loads(line)
                    evidence["evidence_type"] = "entity_page"
                    evidence["evidence_id"] = f"{iterative_number}-{count}"
                    evidences.append(evidence)
                    count += 1
                except:
                    continue

        with open(year_page_path, "r") as fp:
            for line in tqdm(fp):
                try:
                    evidence = json.loads(line)
                    evidence["evidence_type"] = "year_page"
                    evidence["evidence_id"] = f"{iterative_number}-{count}"
                    evidences.append(evidence)
                    count += 1
                except:
                    continue

        main_parts = self._convert_question_from_text(evidences)
        constraint_parts = self._convert_constraint_from_text(evidences)

        self.logger.info(
            f"The total number of main questions from kg + text: {sum([len(value) for key, value in main_parts.items()])}")

        self.logger.info(
            f"The total number of constraint questions from kg + text: {sum([len(value) for key, value in constraint_parts.items()])}")

        main_parts, similar_main_questions = self._group_similar_main_questions(main_parts)

        return main_parts, constraint_parts, similar_main_questions

    def _group_similar_main_questions(self, main_parts):
        similar_main_questions = {}

        for entity, mains in main_parts.items():
            for evidence in mains:
                evidence["embedding"] = self.model.encode(evidence["main_pseudo_question"], convert_to_tensor=True)

        for entity, mains in main_parts.items():
            for evidence in mains:
                evidence["similar_main_ids"] = []
                answer_id = evidence["answer_entity"][0]["id"]
                timespan1 = [evidence["start_time_int"], evidence["end_time_int"]]
                evidences_for_compare = [item for item in mains if item["evidence_id"] != evidence["evidence_id"]]
                for item in evidences_for_compare:
                    timespan2 = [item["start_time_int"], item["end_time_int"]]
                    if timespan1 != timespan2:
                        cosine_similarity = util.pytorch_cos_sim(evidence["embedding"], item["embedding"])
                        similarity_float = cosine_similarity.item()
                        similarity_float = f"{similarity_float:.3f}"
                        # when compute the similarity between evidence from text and others, the similarity threshold is relaxed.
                        if evidence["source"] == "text" or item["source"] == "text":
                            if cosine_similarity > self.text_similar_threshold:
                                evidence["similar_main_ids"].append([item["evidence_id"], similarity_float])
                        elif cosine_similarity > self.similar_threshold:
                            evidence["similar_main_ids"].append([item["evidence_id"], similarity_float])

                if len(evidence["similar_main_ids"]) > 0:
                    if entity not in similar_main_questions:
                        similar_main_questions[entity] = []
                    similar_main_questions[entity].append(evidence)

        for entity, mains in main_parts.items():
            for evidence in mains:
                if "embedding" in evidence:
                    del evidence["embedding"]

        self.logger.info(
            f"The total number of topic entities with temporal sequence information: {len(similar_main_questions)}")

        self.logger.info(
            f"The total number of main questions with temporal sequence information: {sum([len(value) for key, value in similar_main_questions.items()])}")

        return main_parts, similar_main_questions

    def _contain_meaningless_relation(self, evidence):
        evidence_text = evidence["evidence_text"]
        if "USD" in evidence_text:
            return False
        evidence_text = evidence["evidence_text"].lower()
        # - Drop facts with keywords (lowercase before check)
        if "social media followers" in evidence_text:
            return True
        if "population" in evidence_text:
            return True
        if "human development index" in evidence_text:
            return True
        if "wikimedia list article" in evidence_text:
            return True
        if "twinned administrative body" in evidence_text:
            return True
        if "gdp per capita" in evidence_text:
            return True
        if "area code" in evidence_text:
            return True
        if "us$" in evidence_text:
            return True
        if "ppp per capita" in evidence_text:
            return True
        if evidence["source"] == "info" or evidence["source"] == "kb":
            for item in evidence_text.split(KB_ITEM_SEPARATOR):
                if item.lower() == "hdi" or item.lower() == "gdp":
                    return True
        return False

    def _contain_no_relation(self, evidence):
        # - For infobox and text, drop if there are no predicate words
        main_evidence_text = evidence["candidate_question_text"]
        # remove entity from the text
        for mention in [item["disam_label"] for item in evidence["wikidata_entities"] if "disam_label" in item]:
            main_evidence_text = main_evidence_text.replace(mention, "")

        main_evidence_text = remove_all_punctuation(main_evidence_text)
        main_evidence_text = remove_multispace(main_evidence_text)

        # Tokenize the text
        words = word_tokenize(main_evidence_text)

        # Get the list of English stopwords
        stop_words = set(stopwords.words('english'))

        # Remove stopwords from the tokenized words
        filtered_words = [word for word in words if word.lower() not in stop_words]

        if len(filtered_words) < 1:
            return True
        else:
            return False

    def _kb_time_span(self, evidence):
        try:
            timespan = evidence["tempinfo"][0][0]
            start_time = timespan[0].replace("T00:00:00Z", "")
            start_time_int = int(start_time.strip().replace('-', ''))
            end_time = timespan[1].replace("T00:00:00Z", "")
            end_time_int = int(end_time.strip().replace('-', ''))
            return [start_time_int, end_time_int]
        except:
            self.logger.info(
                f"Temporal expression annotation error: {evidence}")

    def _text_time_span(self, evidence):
        try:
            timespan = evidence["tempinfo"][0][0]
            start_time = timespan[0].replace("T00:00:00Z", "")
            end_time = timespan[1].replace("T00:00:00Z", "")
            start_time_int = int(start_time.strip().replace('-', ''))
            end_time_int = int(end_time.strip().replace('-', ''))
            return [start_time_int, end_time_int]
        except:
            self.logger.info(
                f"Temporal expression annotation error: {evidence}")

    def _info_time_span(self, evidence):
        timespans = evidence["tempinfo"][0]
        info_timespan = []
        for item in timespans:
            if item not in info_timespan:
                info_timespan.append(item)
        if len(info_timespan) == 1:
            try:
                start_time = info_timespan[0][0].replace("T00:00:00Z", "")
                end_time = info_timespan[0][1].replace("T00:00:00Z", "")
                start_time_int = int(start_time.strip().replace('-', ''))
                end_time_int = int(end_time.strip().replace('-', ''))
                return [start_time_int, end_time_int]
            except:
                self.logger.info(
                    f"Temporal expression annotation error: {evidence}")
                return None
        elif len(info_timespan) == 2:
            try:
                start_time = min(info_timespan[0][0], info_timespan[1][0]).replace("T00:00:00Z", "")
                end_time = min(info_timespan[0][1], info_timespan[1][1]).replace("T00:00:00Z", "")
                start_time_int = int(start_time.strip().replace('-', ''))
                end_time_int = int(end_time.strip().replace('-', ''))
                return [start_time_int, end_time_int]
            except:
                self.logger.info(
                    f"Temporal expression annotation error: {evidence}")
                return None

    def _convert_question_from_text(self, evidences):
        question_template_for_entity = {}
        for evidence in evidences:
            # drop year page as main question
            if evidence["evidence_type"] == "year_page":
                continue
            # drop evidence without answer
            if len(evidence["answer_entity"]) == 0:
                continue

            retrieved_for_entity = evidence["retrieved_for_entity"]["id"]
            if retrieved_for_entity not in question_template_for_entity:
                question_template_for_entity[retrieved_for_entity] = []

            # drop evidence with meaningless relation
            if self._contain_meaningless_relation(evidence):
                # self.logger.info(f"contain meaningless relation: {evidence}")
                continue

            if evidence["source"] == "info" or evidence["source"] == "text":
                # - For infobox and text, drop if there are no predicate words
                if self._contain_no_relation(evidence):
                    continue

            # kg:"tempinfo": [timespan, disambiguation],
            # text:"tempinfo" = [timespans, timedisambiguations, dates, timetexts, timepositions]
            start_time_int = 0
            end_time_int = 0

            if evidence["source"] == "kb":
                # drop fact if the retrieved entity is not subject
                if not evidence["main_fact"]:
                    continue
                # change time into int number for reasoning
                result = self._kb_time_span(evidence)
                if result:
                    start_time_int = result[0]
                    end_time_int = result[1]
                else:
                    continue

            elif evidence["source"] == "text":
                result = self._text_time_span(evidence)
                if result:
                    start_time_int = result[0]
                    end_time_int = result[1]
                else:
                    continue

            elif evidence["source"] == "info":
                result = self._info_time_span(evidence)
                if result:
                    start_time_int = result[0]
                    end_time_int = result[1]
                else:
                    continue

            if start_time_int < self.min_date or start_time_int > self.max_date:  # minimum date
                continue
            if end_time_int < self.min_date or end_time_int > self.max_date:  # minimum date
                continue

            main_part = {}
            main_part.update({"evidence_id": evidence["evidence_id"]})
            main_part.update({"evidence": evidence["evidence_text"]})
            main_part.update({"source": evidence["source"]})
            main_part.update({"candidate_question_text": evidence["candidate_question_text"]})
            main_part.update({"start_time_int": start_time_int})
            main_part.update({"end_time_int": end_time_int})
            main_part.update({"topic_entity": evidence["retrieved_for_entity"]})
            # if there are multiple answers, choose the first one as the answer
            main_part.update({"answer_entity": [evidence["answer_entity"][0]]})
            # question entity is the entities removing the answer
            main_part.update({"question_entity": [item for item in evidence["wikidata_entities"] if
                                                  item["id"] != main_part["answer_entity"][0]["id"]]})
            if evidence["source"] == "text":
                main_part.update({"text_index": evidence["index"]})

            if evidence["source"] == "text" or evidence["source"] == "info":
                answer_label = main_part["answer_entity"][0]["disam_label"]
            else:
                answer_label = main_part["answer_entity"][0]["label"]

            answer_type_label = main_part["answer_entity"][0]["type"]
            main_question = evidence["candidate_question_text"].replace(answer_label, " ")
            main_question = main_question.replace(".", " ").replace("!", " ").strip()
            main_question = remove_punctuation(main_question)
            main_question = remove_multispace(main_question)
            main_part.update({"main_question_text": main_question})
            main_pseudo_question = f"What {answer_type_label} {main_question}"
            main_pseudo_question = remove_multispace(main_pseudo_question)
            main_part.update({"main_pseudo_question": main_pseudo_question})

            if main_part not in question_template_for_entity[retrieved_for_entity]:
                question_template_for_entity[retrieved_for_entity].append(main_part)

        return question_template_for_entity

    def _convert_constraint_from_text(self, evidences):
        constraint_template_for_entity = {}

        for evidence in evidences:
            retrieved_for_entity = evidence["retrieved_for_entity"]["id"]
            if retrieved_for_entity not in constraint_template_for_entity:
                constraint_template_for_entity[retrieved_for_entity] = []

            if self._contain_meaningless_relation(evidence):
                continue

            start_time_int = 0
            end_time_int = 0

            if evidence["source"] == "kb":
                result = self._kb_time_span(evidence)
                if result:
                    start_time_int = result[0]
                    end_time_int = result[1]
                else:
                    continue

            elif evidence["source"] == "text":
                result = self._text_time_span(evidence)
                if result:
                    start_time_int = result[0]
                    end_time_int = result[1]
                else:
                    continue

            elif evidence["source"] == "info":
                result = self._info_time_span(evidence)
                if result:
                    start_time_int = result[0]
                    end_time_int = result[1]
                else:
                    continue

            if start_time_int < self.min_date or start_time_int > self.max_date:  # minimum date
                continue
            if end_time_int < self.min_date or end_time_int > self.max_date:  # minimum date
                continue

            if evidence["source"] == "info" or evidence["source"] == "text":
                # - For infobox and text, drop if there are no predicate words
                if self._contain_no_relation(evidence):
                    continue

            constraint = {}
            constraint.update({"evidence_id": evidence["evidence_id"]})
            constraint.update({"evidence": evidence["evidence_text"]})
            constraint.update({"source": evidence["source"]})
            constraint.update({"candidate_constraint_text": evidence["candidate_question_text"]})
            constraint_part = evidence["candidate_question_text"]
            constraint_part = constraint_part.replace(".", " ").replace("!", " ").strip()
            constraint_part = remove_punctuation(constraint_part)
            constraint_part = remove_multispace(constraint_part)
            constraint.update({"constraint_text": constraint_part.strip()})
            constraint.update({"start_time_int": start_time_int})
            constraint.update({"end_time_int": end_time_int})
            constraint.update({"topic_entity": evidence["retrieved_for_entity"]})
            constraint.update({"wikidata_entities": evidence["wikidata_entities"]})
            if evidence["source"] == "text":
                constraint.update({"text_index": evidence["index"]})

            if constraint not in constraint_template_for_entity[retrieved_for_entity]:
                # for constraint, only take top-10 evidences
                if evidence["evidence_type"] == "year_page":
                    constraint_template_for_entity[retrieved_for_entity].append(constraint)
                elif evidence["evidence_type"] == "entity_page":
                    if constraint["source"] != "text":
                        constraint_template_for_entity[retrieved_for_entity].append(constraint)
                    elif int(evidence["index"]) <= 10:
                        constraint_template_for_entity[retrieved_for_entity].append(constraint)

        return constraint_template_for_entity
