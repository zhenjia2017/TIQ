import json
import logging
import math
import os
import re
import sys
from pathlib import Path

import nltk
import requests
import yaml
from tqdm import tqdm


def split_time_range(start_year, end_year, interval=15):
    ranges = []
    current_start = start_year

    while current_start <= end_year:
        current_end = min(current_start + interval - 1, end_year)
        ranges.append((current_start, current_end))
        current_start += interval

    reversed_ranges = ranges[::-1]
    return reversed_ranges


def target_question_for_each_range(target_question_total_number, year_range_list, get_year_month_page_link_pool):
    year_pages_num = sum([len(pages) for pages in get_year_month_page_link_pool.values()])
    target_question_for_each_page_num = float(target_question_total_number) / year_pages_num

    target_question_for_each_range = {(current_start, current_end): 0 for (current_start, current_end) in
                                      year_range_list}
    pages_for_each_range = {(current_start, current_end): [] for (current_start, current_end) in
                            year_range_list}

    for (current_start, current_end) in year_range_list:
        num = 0
        for year in range(int(current_start), int(current_end) + 1):
            if year in get_year_month_page_link_pool:
                num += math.ceil(len(get_year_month_page_link_pool[year]) * target_question_for_each_page_num)
                pages_for_each_range[(current_start, current_end)] += get_year_month_page_link_pool[year]
        target_question_for_each_range[(current_start, current_end)] = num
    return target_question_for_each_range, pages_for_each_range


def get_config(path):
    """Load the config dict from the given .yml file."""
    with open(path, "r") as fp:
        config = yaml.safe_load(fp)
    return config


def format_text(text):
    # Transform Unicode-encoded characters to utf-8
    text = re.sub(r'\\u([\d\w]{4})', lambda match: chr(int(match.group(1), 16)), text)
    return text


def store_json_with_mkdir(data, output_path, indent=True):
    """Store the JSON data in the given path."""
    # create path if not exists
    output_dir = os.path.dirname(output_path)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as fp:
        fp.write(json.dumps(data, indent=4))


def store_jsonl_with_mkdir(data, output_path, indent=False):
    """Store the JSON data in the given path."""
    # create path if not exists
    output_dir = os.path.dirname(output_path)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as fp:
        for inst in data:
            fp.write(json.dumps(inst, indent=4 if indent else None))
            fp.write("\n")


def get_qid(wikipedia_link):
    url = f"https://openrefine-wikidata.toolforge.org/en/api?query={wikipedia_link}"
    response = requests.get(url)
    results = response.json()
    if results:
        if "result" in results and results["result"]:
            qid = results["result"][0]['id']
            wikidata_label = results["result"][0]['name']
            return [qid, wikidata_label]


def get_logger(mod_name, config):
    """Get a logger instance for the given module name."""
    # create logger
    logger = logging.getLogger(mod_name)
    # add handler and format
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # set log level
    log_level = config["log_level"]
    logger.setLevel(getattr(logging, log_level))
    return logger


ENT_PATTERN = re.compile("^q[0-9]+$")
YEAR_PATTERN = re.compile("^\d{4}$")
question_words = ["what", "which", "who", "where"]


def filter_ask_number_questions(instance):
    if "how many" in instance["rephrase_question"].lower():
        return True


def filter_question_having_year(instance):
    words = nltk.word_tokenize(instance["rephrase_question"].lower())
    for word in words:
        if YEAR_PATTERN.match(word.strip()):
            return True


def filter_question_contain_qid(instance):
    words = nltk.word_tokenize(instance["rephrase_question"].lower())
    for word in words:
        if ENT_PATTERN.match(word.strip()):
            return True


def filter_ask_time_questions(instance):
    words = instance["rephrase_question"].lower().split()
    if "during what" in instance["rephrase_question"].lower():
        return True
    if "during which" in instance["rephrase_question"].lower():
        return True
    if "what year" in instance["rephrase_question"].lower() and "what yearly" not in instance[
        "rephrase_question"].lower():
        return True
    elif "which month" in instance["rephrase_question"].lower():
        return True
    elif "which year" in instance["rephrase_question"].lower() and "which yearly" not in instance[
        "rephrase_question"].lower():
        return True
    elif "which date" in instance["rephrase_question"].lower():
        return True
    elif "what date" in instance["rephrase_question"].lower():
        return True
    elif "what time" in instance["rephrase_question"].lower():
        return True
    elif "when" in words:
        if len(set(question_words).intersection(set(words))) == 0:
            return True


def filter_questions_have_strange_char(instance):
    if "??" in instance["rephrase_question"].lower():
        return True
    if "(" in instance["rephrase_question"].lower() and ")" not in instance["rephrase_question"].lower():
        print(instance["rephrase_question"])
        return True


def filter_too_many_question_entities(instance, max_question_entity):
    ques_entity_num = []
    for item in instance["question_entity"][0]:
        if item["id"] not in ques_entity_num:
            ques_entity_num.append(item["id"])
    for item in instance["question_entity"][1]:
        if item["id"] not in ques_entity_num:
            ques_entity_num.append(item["id"])
    if len(ques_entity_num) > max_question_entity:
        return True


def reformat_for_select_questions(question_file):
    with open(question_file, "r") as fp:
        questions = json.load(fp)
    results = []
    for instance in tqdm(questions):
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
    with open(question_file.replace(".json", "_format.json"), "w") as fp:
        fp.write(json.dumps(results, indent=4))
    return results
