import json
import os
import random
import re
import signal
import time
from pathlib import Path

from tiq.information_snippet_retrieval.information_snippet_retriever import InformationRetriever
from tiq.library.utils import get_logger
from tiq.pseudo_question_construction.main_constraint_concatenation import MainConstraintConcatenate
from tiq.pseudo_question_construction.main_constraint_generation import MainConstraintGeneration
from tiq.topic_entity_sampling.topic_entity_sampling import TopicEntitySampling

ENT_PATTERN = re.compile("^Q[0-9]+$")

# for processing each sampling, the running time can't be greater than an hour.
MAX_TIME = 7200
# the maximum iteration is 500 for a range
MAX_ITERATION = 500


def myHandler(signum, frame):
    print("time out!!!")


class PseudoQuestionGeneration:
    def __init__(self, config, wp_retriever, year_page_out_dir, year_start, year_end, result_path, topic_entities,
                 target_question_number):
        """Create the pipeline based on the config."""
        # load config
        self.config = config
        self.logger = get_logger(__name__, config)

        self.data_path = self.config["data_path"]
        self.target_question_number = target_question_number
        self.result_path = result_path
        output_path = os.path.join(self.result_path, f"{year_start}_{year_end}_{self.target_question_number}")
        self.output_dir = Path(output_path)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.year_page_out_dir = year_page_out_dir

        self.wp_retriever = wp_retriever

        self.clocq = self.wp_retriever.clocq

        # # year page range for retrieval
        self.year_start = year_start
        self.year_end = year_end

        # record entities that are in rephrased questions
        self.topic_entities = topic_entities

        # create topic entity sampling instance
        self.entity_sampling = TopicEntitySampling(config, self.output_dir, self.year_page_out_dir, self.year_start,
                                                   self.year_end, self.topic_entities)

        # create information snippet retrieval instance
        self.entity_retriever = InformationRetriever(config, self.wp_retriever, self.year_start, self.year_end)

        # create main and constraint parts generation instance
        self.mainconstraint = MainConstraintGeneration(config, self.clocq)

        # create main and constraint concatenation instance
        self.concatenate = MainConstraintConcatenate(config)

        self.pseudo_questions = {}
        self.text_centric_questions = {}
        self.kb_centric_questions = {}

    def sample_statement_for_generation(self, generated_question_file):

        with open(generated_question_file, "r") as fin:
            after_before_overlap_generate_questions = json.load(fin)

        sampled_entity = {}

        for key in after_before_overlap_generate_questions:
            if key not in sampled_entity:
                choice = random.sample(after_before_overlap_generate_questions[key], 1)
                sampled_entity[key] = choice

        self.logger.info(f"sampled entities for generate question: {sampled_entity}")
        return sampled_entity

    def kb_text_central(self, pseudo_questions):
        kb_central_questions = {}
        text_central_questions = {}
        for key, questions in pseudo_questions.items():
            have_source_kb = False
            have_source_text = False
            for item in questions:
                if (item["source"][0] == "kb" or item["source"][1] == "kb") and "text" not in item["source"]:
                    have_source_kb = True
                if (item["source"][0] == "text" or item["source"][1] == "text") and "kb" not in item["source"]:
                    have_source_text = True
            if have_source_kb:
                kb_central_questions.update({key: questions})
            if have_source_text:
                text_central_questions.update({key: questions})
        return kb_central_questions, text_central_questions

    def question_generate_iterative(self):
        iterative_number = 0
        # Iteratively generate pseudo-questions
        # The program terminates when the number of generated pseudo-questions is equal the target number,
        # and the number of the text centric questions is equal to the target number of text centric questions.
        # Since the text centric questions are less few than others (it is more difficult to generate),
        # we target the number of this kind of questions.
        while (len(self.pseudo_questions) < int(self.target_question_number)) and iterative_number < MAX_ITERATION:
            self.logger.info(f"iterative_number: {iterative_number}")
            start = time.time()
            results = self.question_generate_pipeline(iterative_number)
            if results:
                self.pseudo_questions.update(results[0])
                self.topic_entities += results[1]
                kb_central_questions, text_central_questions = self.kb_text_central(results[0])
                self.text_centric_questions.update(text_central_questions)
                self.kb_centric_questions.update(kb_central_questions)
            self.logger.info(f"Time taken for one iteration ({iterative_number}): {time.time() - start} seconds")
            self.logger.info(
                    f"number of kb centric questions in total ({len(self.kb_centric_questions)})")
            self.logger.info(
                    f"number of text centric questions in total ({len(self.text_centric_questions)})")
            self.logger.info(f"Time taken for one iteration ({iterative_number}): {time.time() - start} seconds")
            self.logger.info(f"Rerun the pipeline for this iteration {iterative_number}.")
            iterative_number += 1

        pseudo_question_entity_file = os.path.join(self.output_dir, f'topic_entities_iteration.txt')
        with open(pseudo_question_entity_file, 'w') as fo:
            for entity in list(self.pseudo_questions.keys()):
                fo.write(entity)
                fo.write("\n")

        pseudo_question_file = os.path.join(self.output_dir, f'pseudo_questions_iteration.json')
        with open(pseudo_question_file, "w") as fout:
            fout.write(json.dumps(self.pseudo_questions, indent=4))

        self.entity_retriever.store_dump()

    def question_generate_pipeline(self, iterative_number):
        iterative_output_path = os.path.join(self.output_dir, f"i{iterative_number}")
        iterative_output_dir = Path(iterative_output_path)
        iterative_output_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"iterative_number: {iterative_number}")

        sample_entity_file = os.path.join(iterative_output_dir, f'entity_sample.jsonl')

        pseudo_question_entity_file = os.path.join(iterative_output_dir, f'pseudo_question_entity.txt')

        entity_information_file = os.path.join(iterative_output_dir, f'entity_information.jsonl')

        main_part_file = os.path.join(iterative_output_dir, f"main_part.json")
        constraint_part_file = os.path.join(iterative_output_dir, f"constraint_part.json")

        temporal_sequence_main_part_file = os.path.join(iterative_output_dir, f"main_part_temporal_sequence.json")

        pseudo_question_file = os.path.join(iterative_output_dir, f'pseudo_questions.json')

        merged_similar_pseudo_question_file = os.path.join(iterative_output_dir, f'pseudo_question_merge_similar.json')

        # sample entities
        sampled_entity = self.entity_sampling.sample_entity_for_retrieval()
        retrieve_entities = []
        for key in sampled_entity:
            retrieve_entities += sampled_entity[key]

        if len(retrieve_entities) == 0:
            return None

        # store the sampled entities
        with open(sample_entity_file, 'w') as fp:
            for entity in retrieve_entities:
                fp.write(json.dumps(entity))
                fp.write("\n")

        # retrieve information snippet for the sampled entities
        sample_entity_evidences = self.retrieve_entity_page(retrieve_entities)

        # store the retrieval results
        with open(entity_information_file, 'w') as fp:
            for event in sample_entity_evidences:
                fp.write(json.dumps(event))
                fp.write("\n")

        # construct main and constraint
        main_parts, constraint_parts, similar_main_questions = self.mainconstraint.main_constraint_generation(
            entity_information_file,
            self.entity_sampling.year_evidence_file, iterative_number)
        with open(main_part_file, "w") as fm:
            fm.write(json.dumps(main_parts, indent=4))

        with open(constraint_part_file, "w") as fm:
            fm.write(json.dumps(constraint_parts, indent=4))

        with open(temporal_sequence_main_part_file, "w") as fm:
            fm.write(json.dumps(similar_main_questions, indent=4))

        # concatenate main and constraint
        pseudo_questions = self.concatenate.concatenate_main_constraint_semantic_base(temporal_sequence_main_part_file,
                                                                                      constraint_part_file)
        pseudo_questions_entities = list(pseudo_questions.keys())

        with open(pseudo_question_entity_file, 'w') as fp:
            for entity in pseudo_questions_entities:
                fp.write(entity)
                fp.write("\n")

        with open(pseudo_question_file, "w") as fout:
            fout.write(json.dumps(pseudo_questions, indent=4))

        merged_pseudo_questions = self.merge_similar_pseudo_questions(pseudo_questions)

        with open(merged_similar_pseudo_question_file, 'w') as fout:
            fout.write(json.dumps(merged_pseudo_questions, indent=4))

        return [merged_pseudo_questions, pseudo_questions_entities]

    def merge_similar_pseudo_questions(self, pseudo_questions):
        merged_pseudo_questions = {}

        for key, questions in pseudo_questions.items():
            questions_dic = {}
            have_similar_dic = {}
            for instance in questions:
                main_k = key + "||" + instance["main_evidence_id"] + "||" + instance["signal"] + "||" + instance[
                    "constraint_evidence_id"]
                questions_dic[main_k] = instance
                similar_id_score = {}
                if "similar_main_ids" in instance:
                    for item in instance["similar_main_ids"]:
                        similar_id_score[item[0]] = float(item[1])
                if "similar_main" in instance and len(instance["similar_main"]) > 0:
                    if main_k not in have_similar_dic:
                        have_similar_dic[main_k] = []
                    for main_evidence_id in instance["similar_main"]:
                        simi_k = key + "||" + main_evidence_id + "||" + instance["signal"] + "||" + instance[
                            "constraint_evidence_id"]
                        have_similar_dic[main_k].append(simi_k)

            is_others_similar = list(have_similar_dic.values())

            total_questions = []
            questions_copy = questions.copy()
            for instance in questions_copy:
                main_k = key + "||" + instance["main_evidence_id"] + "||" + instance["signal"] + "||" + instance[
                    "constraint_evidence_id"]
                if main_k in have_similar_dic:
                    instance["similar_pseudo_question"] = []
                    for main_evidence_id in have_similar_dic[main_k]:
                        merge_question = {}
                        merge_question["pseudo_question_construction"] = questions_dic[main_evidence_id][
                            "pseudo_question_construction"]
                        merge_question["evidence"] = questions_dic[main_evidence_id]["evidence"][0]
                        merge_question["source"] = questions_dic[main_evidence_id]["source"][0]
                        merge_question["timespan"] = questions_dic[main_evidence_id]["timespan"][0]
                        merge_question["question_entity"] = questions_dic[main_evidence_id]["question_entity"][0]
                        merge_question["answer"] = questions_dic[main_evidence_id]["answer"]
                        merge_question["main_evidence_id"] = questions_dic[main_evidence_id]["main_evidence_id"]
                        instance["similar_pseudo_question"].append(merge_question)
                    total_questions.append(instance)
                else:
                    if main_k not in is_others_similar:
                        total_questions.append(instance)

            if total_questions:
                merged_pseudo_questions.update({key: total_questions})

        print("number of entities before merge: ", len(pseudo_questions))
        print("number of entities after merge: ", len(merged_pseudo_questions))
        print("total number of questions before merge: ", sum([len(item) for item in list(pseudo_questions.values())]))
        print("number of questions after merge: ",
              sum([len(item) for item in list(merged_pseudo_questions.values())]))

        return merged_pseudo_questions

    def retrieve_entity_page(self, sample_entities):
        start = time.time()
        sample_entity_evidences = []
        for entity in sample_entities:
            evidences = self.entity_retriever.retrieve_evidences_from_heterogeneous_sources(entity)
            sample_entity_evidences += evidences
        print("Time consumed", time.time() - start)
        return sample_entity_evidences
