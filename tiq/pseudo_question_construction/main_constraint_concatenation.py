# connect main and constraint with temporal conjunctions via temporal reasoning, etc.
# input: main candidates' file and constraint candidates' file
# output: pseudo-questions
import json
import re
import time

from tiq.library.temporal_annotator.spacy_tokenizer import SpacyTokenizer
from tiq.library.utils import get_logger, format_text

KB_ITEM_SEPARATOR = ", "
relation_word_map = {"BEFORE": "before", "AFTER": "after", "OVERLAP": "during"}


def remove_multispace(text):
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def format_text(text):
    # Transform Unicode-encoded characters to utf-8
    text = re.sub(r'\\u([\d\w]{4})', lambda match: chr(int(match.group(1), 16)), text)
    return text


class MainConstraintConcatenate:
    def __init__(self, config):
        # load or generate frequency for each qid
        self.config = config
        self.logger = get_logger(__name__, config)
        self.tokenizer = SpacyTokenizer(config)
        self.max_pseudo_question_length = self.config["max_pseudo_question_length"]

    def reason_overlap(self, main_timespan, constraint_timespan):
        evi_begin = main_timespan[0]
        evi_end = main_timespan[1]
        constraint_start = constraint_timespan[0]
        constraint_end = constraint_timespan[1]

        if main_timespan == constraint_timespan:
            return True

        # When temporal values in both evidence and constraint are the year granularity, change the
        # time point of beginning and end back into a year+0101. E.g., [20020101, 20231231]-->[20020101, 20230101]
        if "0101" in str(evi_begin)[-4:] and "1231" in str(evi_end)[-4:] and "0101" in str(constraint_start)[
                                                                                       -4:] and "1231" in str(
            constraint_end)[-4:]:
            # evi_end is a year and constraint_start is a year
            evi_end = int(str(evi_end)[:-4] + "0101")
            constraint_end = int(str(constraint_end)[:-4] + "0101")

        # In case that the temporal value in evidence is a duration begin time point or the end time point of evidence and constraint are same
        if constraint_start <= evi_begin and constraint_end >= evi_end:
            # temporal value of constraint includes temporal value of evidence
            return True
        if evi_begin <= constraint_start and evi_end >= constraint_end:
            # temporal value of evidence includes temporal value of constraint
            return True
        if evi_begin <= constraint_start and evi_end > constraint_start and evi_end <= constraint_end:
            # temporal values are overlap and start time of evidence is less than the start time of constraint
            return True
        if evi_end >= constraint_end and evi_begin > constraint_start and evi_begin < constraint_end:
            # temporal values are overlap and start time of constraint is less than the start time of evidence
            return True

    def is_year(self, start, end):
        if "0101" in str(start)[-4:] and "1231" in str(end)[-4:] and str(start)[0:4] == str(end)[0:4]:
            return True

    def reason_after(self, main_timespan, constraint_timespan):
        evi_begin = main_timespan[0]
        evi_end = main_timespan[1]
        constraint_start = constraint_timespan[0]
        constraint_end = constraint_timespan[1]

        if main_timespan == constraint_timespan:
            return False

        if "0101" in str(constraint_start)[-4:] and "1231" in str(constraint_end)[-4:] and "0101" in str(evi_begin)[
                                                                                                         -4:]:
            # temporal value of constraint is a year/years and start time in evidence is a year, change the temporal value
            # of constraint into year granularity
            constraint_end = int(str(constraint_end)[:-4] + "0101")

        if evi_begin > constraint_end and evi_begin == evi_end and evi_begin - constraint_end < 3:
            # For before/after: timespans not more than 1-2 years apart
            # after relation in case the temporal value in evidence is a specific year or a specific day
            # immediate after is not allowed
            return True
        if evi_begin >= constraint_end and evi_begin < evi_end and evi_begin - constraint_end < 3:
            # For before/after: timespans not more than 1-2 years apart
            # after relation in case the temporal value in evidence is a duration
            # immediate after is allowed
            return True

    def reason_before(self, main_timespan, constraint_timespan):
        evi_begin = main_timespan[0]
        evi_end = main_timespan[1]
        constraint_start = constraint_timespan[0]
        constraint_end = constraint_timespan[1]

        if main_timespan == constraint_timespan:
            # same temporal values of constraint and evidence never have before relation
            return False

        if "0101" in str(evi_begin)[-4:] and "1231" in str(evi_end)[-4:] and "0101" in str(constraint_start)[-4:]:
            # when temporal value of evidence is year/years and start time of constraint is a year, change the temporal value of
            # evidence back to year granularity.
            evi_end = int(str(evi_end)[:-4] + "0101")

        if evi_end < constraint_start and evi_begin == evi_end and constraint_start - evi_end < 3:
            # For before/after: timespans not more than 1-2 years apart
            # before relation in case the temporal value in evidence is a specific year or a specific day
            # immediate before is not allowed
            return True
        if evi_end <= constraint_start and evi_begin < evi_end and constraint_start - evi_end < 3:
            # For before/after: timespans not more than 1-2 years apart
            # before relation in general situations
            # immediate before is allowed only in the case that the temporal value in evidence is a duration
            return True

    def reason_signal(self, main_timespan, constraint_timespan):
        if self.reason_after(main_timespan, constraint_timespan):
            return "AFTER"
        elif self.reason_before(main_timespan, constraint_timespan):
            return "BEFORE"
        elif self.reason_overlap(main_timespan, constraint_timespan):
            return "OVERLAP"
        else:
            return None


    def get_pair(self, group1, group2):
        pairs = []
        for item1 in group1:
            for item2 in group2:
                pairs.append((item1, item2))
        return pairs

    def check_main_constraint_connectivity(self, pairs, connect_pair):
        for pair in pairs:
            if (pair[0], pair[1]) in connect_pair:
                return connect_pair[(pair[0], pair[1])]
            if (pair[1], pair[0]) in connect_pair:
                return connect_pair[(pair[1], pair[0])]
        return 0.0

    def check_pair_connectivity(self, pairs, connect_pair):
        for pair in pairs:
            if (pair[0], pair[1]) in connect_pair or (pair[1], pair[0]) in connect_pair:
                continue
            connectivity = self.clocq.connectivity_check(pair[0], pair[1])
            if connectivity > 0:
                connect_pair[(pair[0], pair[1])] = float(connectivity)

    def check_have_same_fact(self, part1, part2, main_timespan, constraint_timespan, main_entity, constraint_entity):
        # check according to surface words
        part1_words = self.tokenizer.tokenize(part1.lower()).lemmas()
        part2_words = self.tokenizer.tokenize(part2.lower()).lemmas()
        if (
                "die" in part1_words or "death" in part1_words or "dead" in part1_words) and "date of death" in part2.lower():
            return True
        elif (
                "die" in part2_words or "death" in part2_words or "dead" in part2_words) and "date of death" in part1.lower():
            return True
        elif (
                "die" in part1_words or "death" in part1_words or "dead" in part1_words) and "death date" in part2.lower():
            return True
        elif (
                "die" in part2_words or "death" in part2_words or "dead" in part2_words) and "death date" in part1.lower():
            return True
        elif ("bear" in part1_words or "birth" in part1_words) and "birthdate" in part2_words:
            return True
        elif ("bear" in part2_words or "birth" in part2_words) and "birthdate" in part1_words:
            return True
        elif ("bear" in part1_words or "birth" in part1_words) and "date of birth" in part2.lower():
            return True
        elif ("bear" in part2_words or "birth" in part2_words) and "date of birth" in part1.lower():
            return True

        # check according to same entity and same date
        if main_timespan == constraint_timespan or abs(main_timespan[0] - constraint_timespan[0]) < 2 or abs(
                main_timespan[1] - constraint_timespan[1]) < 2:
            # "Death of Abu Bakr al-Baghdadi, Statement from the President on the Death of Abu Bakr al-Baghdadi, October 27, 2019.",
            # "Death of Abu Bakr al-Baghdadi, point in time, 26 October \"2019"
            if ("bear" in part1_words or "birth" in part1_words) and ("bear" in part2_words or "birth" in part2_words):
                return True
            elif ("die" in part1_words or "death" in part1_words or "dead" in part1_words) and (
                    "die" in part2_words or "death" in part2_words or "dead" in part2_words):
                return True
            elif "spouse" in part1_words and "spouse" in part2_words:
                return True
            else:
                main_entities = set([item["id"] for item in main_entity if item["id"][0] == "Q"])
                constraint_entities = set([item["id"] for item in constraint_entity if item["id"][0] == "Q"])
                if main_entities == constraint_entities:
                    return True

    def concatenate_main_constraint_semantic_base(self, main_part_file, constraint_part_file):
        # concatenate two part
        with open(main_part_file, "r") as fin:
            main_questions = json.load(fin)

        with open(constraint_part_file, "r") as fin:
            constraint_parts = json.load(fin)

        start = time.time()
        pseudo_question_per_entity = {}
        for retrieved_for_entity, main_instances in main_questions.items():
            # for each constraint, we randomly select main questions
            pseudo_question_per_entity[retrieved_for_entity] = []
            pseudo_questions = []
            for main_instance in main_instances:
                # ignore the main part if there are no temporal sequence information
                if len(main_instance["similar_main_ids"]) == 0: continue
                main_question_text = main_instance["main_question_text"]
                main_pseudo_question = main_instance["main_pseudo_question"]
                main_timespan = [main_instance["start_time_int"], main_instance["end_time_int"]]
                for key, constraint_instances in constraint_parts.items():
                    for constraint_instance in constraint_instances:

                        if len(main_question_text.split()) + len(
                                constraint_instance["constraint_text"].split()) > self.max_pseudo_question_length:
                            continue

                        constraint_timespan = [constraint_instance["start_time_int"],
                                               constraint_instance["end_time_int"]]
                        # drop main questions having the same fact with constraint
                        if main_instance["evidence_id"] == constraint_instance["evidence_id"]: continue
                        # drop main questions having die born and constraint having date of death, date of birth and vice versa
                        # drop the constraint parts that contain the entities in answers
                        if set([item["id"] for item in main_instance["answer_entity"]]).intersection(
                                set([item["id"] for item in constraint_instance["wikidata_entities"]])):
                            continue


                        semantic_type = 0.0
                        if set([item["id"] for item in constraint_instance["wikidata_entities"]]).intersection(
                                set([item["id"] for item in main_instance["question_entity"]])):
                            semantic_type = 2.0

                        if semantic_type != 2.0:
                            continue

                        if self.check_have_same_fact(main_instance["main_question_text"],
                                                     constraint_instance["constraint_text"], main_timespan,
                                                     constraint_timespan,
                                                     main_instance["question_entity"],
                                                     constraint_instance["wikidata_entities"]):
                            continue

                        signal = self.reason_signal(main_timespan, constraint_timespan)
                        # we take care of before after in another function
                        if not signal: continue

                        generate_question = {}
                        pseudo_question = f'{main_pseudo_question}, {relation_word_map[signal]}, {constraint_instance["constraint_text"]}'
                        pseudo_question = remove_multispace(pseudo_question)
                        generate_question["pseudo_question_construction"] = format_text(pseudo_question).encode(
                            'utf-8').decode('utf-8')
                        generate_question["semantic_type"] = semantic_type
                        generate_question["signal"] = signal

                        generate_question["evidence"] = [main_instance["evidence"], constraint_instance["evidence"]]
                        generate_question["source"] = [main_instance["source"], constraint_instance["source"]]
                        generate_question["timespan"] = [main_timespan, constraint_timespan]
                        generate_question["topic_entity"] = main_instance["topic_entity"]
                        generate_question["question_entity"] = [main_instance["question_entity"],
                                                                constraint_instance["wikidata_entities"]]
                        generate_question["answer"] = main_instance["answer_entity"]
                        generate_question["main_evidence_id"] = main_instance["evidence_id"]
                        generate_question["constraint_evidence_id"] = constraint_instance["evidence_id"]
                        generate_question["similar_main_ids"] = main_instance["similar_main_ids"]
                        if generate_question not in pseudo_questions:
                            pseudo_questions.append(generate_question)

            pseudo_question_per_entity[retrieved_for_entity] += pseudo_questions
            if len(pseudo_question_per_entity[retrieved_for_entity]) == 0:
                del pseudo_question_per_entity[retrieved_for_entity]
            self.logger.debug(f"Signal reasoning finish for one entity: {retrieved_for_entity}")

        self.logger.info(f"Time taken (signal reasoning): {time.time() - start} seconds")
        self.find_similar_main_with_same_constraint(pseudo_question_per_entity)

        return pseudo_question_per_entity

    def find_similar_main_with_same_constraint(self, pseudo_question_per_entity):
        for entity, pseudo_questions in pseudo_question_per_entity.items():
            for i in range(len(pseudo_questions) - 1):
                constraint_evidence_id = pseudo_questions[i]["constraint_evidence_id"]
                similar_evidence_ids = [item[0] for item in pseudo_questions[i]["similar_main_ids"]]
                signal_i = pseudo_questions[i]["signal"]
                pseudo_questions[i]["similar_main"] = []
                for j in range(i + 1, len(pseudo_questions)):
                    signal_j = pseudo_questions[j]["signal"]
                    if constraint_evidence_id == pseudo_questions[j]["constraint_evidence_id"] and signal_i == signal_j:
                        if pseudo_questions[j]["main_evidence_id"] in similar_evidence_ids:
                            pseudo_questions[i]["similar_main"].append(pseudo_questions[j]["main_evidence_id"])
