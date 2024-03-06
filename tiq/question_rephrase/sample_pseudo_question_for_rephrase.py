import random
from collections import Counter

from tiq.library.utils import *
from tiq.question_rephrase.question_rephraser import QuestionRephrase


class PseudoQuestionSampleRephrase:
    def __init__(self, config):
        # load config
        self.config = config
        self.logger = get_logger(__name__, config)

        # The folder for storing the intermediate results
        self.result_path = self.config["result_path"]
        self.year_start = self.config["year_start"]
        self.year_end = self.config["year_end"]
        self.target_question_total_number = self.config["target_question_number"]
        self.sample_size = self.config["sample_size"]
        self.output_dir = os.path.join(self.result_path,
                                       f"{self.year_start}_{self.year_end}_{self.target_question_total_number}_{self.sample_size}")
        self.long_tail_entity_frequency = self.config["long_tail_entity_frequency"]
        self.prominent_entity_frequency = self.config["prominent_entity_frequency"]
        # folder of the pseudo-questions
        self.pseudo_questions_file_path = os.path.join(self.output_dir, self.config["pseudo_questions_in_total_file"])
        # create question rephrasing instance
        self.rephrase_gpt = QuestionRephrase(config)

    def sample_and_rephrase_pseudo_questions(self):
        with open(self.pseudo_questions_file_path, "r") as fp:
            # pseudo-questions pool for rephrasing
            pseudo_questions = json.load(fp)

        # for each topic entity, we only sample one pseudo-question for rephrase
        sample_questions, rephrased_questions, filered_rephrased_questions = self.sample_one_question_for_rephrasing(
            pseudo_questions)

        self.rephrase_gpt.store_cache()

        return sample_questions, rephrased_questions, filered_rephrased_questions

    def sample_one_question_for_rephrasing(self, pseudo_questions):
        self.logger.info(f"number of topic entities: {len(pseudo_questions)}")
        # we assign more weight for text centric questions when sampling pseudo questions
        source_weight, signal_weight = self.distribution_pseudo_questions(pseudo_questions)
        sample_questions_result = self.sample_questions(pseudo_questions, source_weight)
        self.logger.info(f"number of sampled questions: {len(sample_questions_result)}")
        rephrased_questions_result = self.rephrase_questions(sample_questions_result)
        self.logger.info(f"number of rephrased questions: {len(rephrased_questions_result)}")
        filered_rephrased_questions = self.filter_noise_rephrase_questions(rephrased_questions_result)
        self.logger.info(f"number of rephrased questions after filtering: {len(filered_rephrased_questions)}")
        return sample_questions_result, rephrased_questions_result, filered_rephrased_questions

    def sample_according_to_source(self, questions_for_source):
        sample_one_question_balance = random.sample(questions_for_source, 1)[0]
        return sample_one_question_balance

    def gpt_rephrase(self, instance):
        rephrase_result = self.rephrase_gpt.rephrase_on_instance(instance)

        if rephrase_result:
            rephrased_question = rephrase_result[0]
            rephrased_question_length = rephrase_result[1]
            check_signal = rephrase_result[2]

            instance["rephrase_question"] = rephrased_question
            instance["rephrase_question_length"] = rephrased_question_length
            instance["check_signal"] = check_signal

            return instance

    def sample_questions(self, pseudo_questions, source_weight):
        sample_questions = []
        for key, questions in pseudo_questions.items():

            sources = {}
            select_source_weight = {}
            for item in questions:
                source = "; ".join(item["source"])
                if source not in sources:
                    sources[source] = []
                sources[source].append(item)

            # sample source according to the source priority: fewer, higher
            select_source_weight.update({source: source_weight[source] for source in sources})
            sample_source = weighted_random_choice(select_source_weight)
            sample_one_question_balance = self.sample_according_to_source(sources[sample_source])
            sample_questions.append(sample_one_question_balance)

        return sample_questions

    def rephrase_questions(self, sample_questions):
        rephrased_questions_result = []
        for instance in sample_questions:
            result = self.gpt_rephrase(instance)
            if result:
                rephrased_questions_result.append(result)

        return rephrased_questions_result

    def filter_noise_rephrase_questions(self, rephrase_questions):
        filtered_instances = []
        for instance in rephrase_questions:
            if "similar_pseudo_question" in instance:
                instance = merge_similar_main_answer(instance)
            if instance["rephrase_question_length"] < self.config["min_token_length"]:
                self.logger.info(f"drop the rephrased questions with length less than minimum token length")
                continue
            if len(instance["answer"]) > self.config["max_answer_entity"]:
                self.logger.info(f"drop the rephrased questions with answer greater than 10")
                continue
            if filter_question_having_year(instance):
                self.logger.info(f"drop the rephrased questions having year")
                continue
            if filter_question_contain_qid(instance):
                self.logger.info(f"drop the rephrased questions having qid")
                continue
            if filter_questions_have_strange_char(instance):
                self.logger.info(f"drop the rephrased questions having strange char")
                continue
            if filter_too_many_question_entities(instance, self.config["max_question_entity"]):
                self.logger.info(f"drop the rephrased questions with too many question entities")
                continue
            if filter_ask_number_questions(instance):
                self.logger.info(f"drop the rephrased questions asking for numbers")
                continue
            if filter_ask_time_questions(instance):
                self.logger.info(f"drop the rephrased questions asking for time")
                continue
            if instance not in filtered_instances:
                filtered_instances.append(instance)

        self.logger.info(f"Number of good rephrased questions: {len(filtered_instances)}")
        return filtered_instances

    def distribution_pseudo_questions(self, pseudo_questions):

        sources_total = {}
        signals_total = {}

        source_per_key_total = []
        signal_per_key_total = []

        for key, questions in pseudo_questions.items():
            sources = {}
            signals = {}
            for item in questions:
                source = "; ".join(item["source"])
                if source not in sources:
                    sources[source] = []

                if source not in sources_total:
                    sources_total[source] = 0

                sources[source].append(item)

                if item["signal"] not in signals:
                    signals[item["signal"]] = []

                if item["signal"] not in signals_total:
                    signals_total[item["signal"]] = 0

                signals[item["signal"]].append(item)

            sources_total.update({source: sources_total[source] + len(sources[source]) for source in sources})
            signals_total.update({signal: signals_total[signal] + len(signals[signal]) for signal in signals})

            source_per_key_total += list(sources.keys())
            signal_per_key_total += list(signals.keys())

        print("number of topic entities: ", len(pseudo_questions))

        source_item_counts = count_items(source_per_key_total)
        signal_item_counts = count_items(signal_per_key_total)

        source_count = {}
        source_weight = {}

        signal_count = {}
        signal_weight = {}

        source_item_counts_sum = sum(source_item_counts.values())
        for item, count in source_item_counts.items():
            print(f"Item {item} appears {count} times in the list.")
            source_count[item] = 1 / count / source_item_counts_sum

        source_count_sum = sum(source_count.values())
        for item in source_count:
            source_weight[item] = source_count[item] / source_count_sum

        signal_item_counts_sum = sum(signal_item_counts.values())
        for item, count in signal_item_counts.items():
            print(f"Item {item} appears {count} times in the list.")
            signal_count[item] = 1 / count / signal_item_counts_sum

        signal_count_sum = sum(signal_count.values())
        for item in signal_count:
            signal_weight[item] = signal_count[item] / signal_count_sum

        for item, weight in source_weight.items():
            print(f"Item {item} weight {weight}")

        for item, weight in signal_weight.items():
            print(f"Item {item} weight {weight}")

        for item, count in sources_total.items():
            print(f"Item {item} appears {count} times in the list.")

        for item, count in signals_total.items():
            print(f"Item {item} appears {count} times in the list.")

        return source_weight, signal_weight


def count_items(lst):
    item_counts = Counter(lst)
    return item_counts


def weighted_random_choice(item_weight_dic):
    items = []
    weights = []
    for item, weight in item_weight_dic.items():
        items.append(item)
        weights.append(weight)

    cumulative_weights = [0]
    total_weight = 0

    for weight in weights:
        total_weight += weight
        cumulative_weights.append(total_weight)

    random_value = random.uniform(0, total_weight)

    for i, weight in enumerate(cumulative_weights):
        if random_value < weight:
            return items[i - 1]


def merge_similar_main_answer(instance):
    for item in instance["similar_pseudo_question"]:
        instance["answer"] += item["answer"]
        instance["evidence"].insert(1, item["evidence"])
        instance["timespan"].insert(1, item["timespan"])
        instance["source"].insert(1, item["source"])
        for ques_entity in item["question_entity"]:
            if ques_entity not in instance["question_entity"][0]:
                instance["question_entity"][0].append(ques_entity)
        instance["main_evidence_id"] = [instance["main_evidence_id"]]
        instance["main_evidence_id"].append(item["main_evidence_id"])
    return instance


def benchmark_format_for_select_questions(questions):
    results = []
    for instance in questions:
        new_format = {}
        new_format["rephrase_question"] = instance["rephrase_question"]
        new_format["pseudo_question_construction"] = instance["pseudo_question_construction"]
        new_format["evidence"] = {"main": [], "constraint": []}
        constraint_evidence = instance["evidence"][-1]
        main_evidences = instance["evidence"][:-1]
        constraint_source = instance["source"][-1]
        main_sources = instance["source"][:-1]
        constraint_timespan = instance["timespan"][-1]
        main_timespans = instance["timespan"][:-1]
        new_format["evidence"]["constraint"].append([constraint_evidence, constraint_source, constraint_timespan])
        for evidence in main_evidences:
            idx = main_evidences.index(evidence)
            new_format["evidence"]["main"].append([evidence, main_sources[idx], main_timespans[idx]])
        new_format["signal"] = instance["signal"]
        new_format["topic_entity"] = instance["topic_entity"]
        if type(instance["topic_entity"]["type"]) == list:
            new_format["topic_entity"]["type"] = new_format["topic_entity"]["type"][0]
        new_format["question_entity"] = []
        question_entity_ids = []
        for item in instance["question_entity"][0] + instance["question_entity"][1]:
            if item["id"] not in question_entity_ids:
                question_entity_ids.append(item["id"])
                new_format["question_entity"].append({"id": item["id"], "label": item["label"]})
        answer_ids = []
        new_format["answer"] = []
        for item in instance["answer"]:
            if item["id"] not in answer_ids:
                answer_ids.append(item["id"])
                new_format["answer"].append({"id": item["id"], "label": item["label"]})

        results.append(new_format)
    return results
