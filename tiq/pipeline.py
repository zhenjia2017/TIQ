'''
The benchmark automatically construction pipeline mainly includes three stages:
Stage 1: retrieve year pages: retrieve year pages for getting started with
Stage 2: construct pseudo-questions: include (i) topic entity sampling, (ii) information snippet retrieval and (iii) pseudo-question construction
Stage 3: rephrase pseudo-questions: question rephrasing via InstructGPT
'''

import json
import os
import pickle
import sys
import time
from pathlib import Path

from clocq.CLOCQ import CLOCQ
from clocq.interface.CLOCQInterfaceClient import CLOCQInterfaceClient

from tiq.information_snippet_retrieval.wp_retriever.wikipedia_entity_retriever import WikipediaEntityPageRetriever
from tiq.library.utils import get_config, get_logger, get_qid, split_time_range, target_question_for_each_range
from tiq.pseudo_question_construction.pseudo_question_generation import PseudoQuestionGeneration
from tiq.question_rephrase.sample_pseudo_question_for_rephrase import PseudoQuestionSampleRephrase
from tiq.year_page_retrieval.year_page_retriever import YearPageRetrieval

EVENT_PAGE_PREFIX = "Portal:Current_events"


class Pipeline:
    def __init__(self, config):
        """Initialize the year range for getting started with,
        generate year/month page urls,
        split year/month pages urls into groups in which each group span a time interval such as 50 years,
        and other configurations."""

        # load config
        self.config = config
        self.logger = get_logger(__name__, config)

        # define months in each year
        self.months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
                       "November", "December"]

        # load the start year and end year. It defines the whole time span for retrieval information/events.
        self.year_start = self.config["year_start"]
        self.year_end = self.config["year_end"]

        # load the data path and result path
        self.data_path = self.config["data_path"]
        self.result_path = self.config["result_path"]

        # load wikidata qid and wikipedia url mapping dictionary for generating year/month qid and urls
        with open(os.path.join(self.config["data_path"], self.config["path_to_wikidata_mappings"]), "rb") as fp:
            self.wikidata_mappings = pickle.load(fp)
        with open(os.path.join(self.config["data_path"], self.config["path_to_wikipedia_mappings"]), "rb") as fp:
            self.wikipedia_mappings = pickle.load(fp)

        # load year pages storing path
        year_page_output_path = os.path.join(self.result_path, f"{self.year_start}_{self.year_end}_year_page")
        self.year_page_out_dir = Path(year_page_output_path)
        self.year_page_out_dir.mkdir(parents=True, exist_ok=True)

        # generate (or load if there already is) year/month qids and urls
        year_page_qid_url_file = os.path.join(self.result_path,
                                              f"{self.year_start}_{self.year_end}_year_page_qid.pickle")
        if os.path.exists(year_page_qid_url_file):
            with open(year_page_qid_url_file, "rb") as fyear:
                self.get_year_month_page_link_pool = pickle.load(fyear)

        else:
            # generate year/month page qid and url mappings according to the start and end year range configuration
            # beyond the year pages, we go to the granularity of months, e.g.
            # https://en.wikipedia.org/wiki/Portal:Current_events/January_2022
            # or https://en.wikipedia.org/wiki/March_2022
            self.get_year_month_page_link_pool = self._year_page_pool(self.year_start, self.year_end)

            with open(year_page_qid_url_file, "wb") as fyear:
                pickle.dump(self.get_year_month_page_link_pool, fyear)

        self.logger.info(f"total number of target year range: {len(self.get_year_month_page_link_pool)}")
        self.logger.info(f"target years: {self.get_year_month_page_link_pool.keys()}")
        self.year_pages_num = sum([len(pages) for pages in self.get_year_month_page_link_pool.values()])
        self.logger.info(f"total number of year and month pages: {self.year_pages_num}")

        # the target number of pseudo-questions for generating in total
        self.target_question_total_number = self.config["target_question_number"]
        # set the sample size of entities, 150 as default.
        self.sample_size = self.config["sample_size"]
        # set year range interval, 50 years as default.
        # the year range is split into groups with time range interval.
        # the pseudo-question construction pipeline is within the time range interval so that the entity pool is not too large.
        self.year_range_interval = self.config["year_range_interval"]
        self.year_range_list = split_time_range(self.year_start, self.year_end, self.year_range_interval)
        # the total target number of questions is distributed in each group of the time range interval
        # if the total target number of questions is less than the total number of pages, we use the total number of pages to replace the total target number of questions
        self.target_question_number_per_range, self.pages_for_each_range = target_question_for_each_range(
            self.target_question_total_number, self.year_range_list, self.get_year_month_page_link_pool)

        self.logger.info(f"year range: {self.year_range_list}")
        self.logger.info(f"target questions number for each year range: {self.target_question_number_per_range}")

        # create the folder for storing the intermediate results
        output_path = os.path.join(self.result_path,
                                   f"{self.year_start}_{self.year_end}_{self.target_question_total_number}_{self.sample_size}")
        self.output_dir = Path(output_path)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # topic entities
        self.topic_entities_file_path = os.path.join(self.output_dir, self.config["topic_entity_in_total_file"])
        self.pseudo_questions_file_path = os.path.join(self.output_dir, self.config["pseudo_questions_in_total_file"])

        self.topic_entities_in_total = []
        # for avoiding the duplicated topic entities, we store the topic entities and initialize via reading the file.
        # after generating the pseudo-questions, the file will be updated.
        if os.path.exists(self.topic_entities_file_path):
            with open(self.topic_entities_file_path, "r") as fp:
                for item in fp.readlines():
                    self.topic_entities_in_total.append(item.strip())

        # initialize the generated pseudo-questions
        self.pseudo_question_in_total = {}

        if config["clocq_use_api"]:
            self.clocq = CLOCQInterfaceClient(host=config["clocq_host"], port=config["clocq_port"])
        else:
            self.clocq = CLOCQ()

        # instantiate wikipedia retriever
        self.wp_retriever = WikipediaEntityPageRetriever(config, self.clocq, self.wikidata_mappings,
                                                         self.wikipedia_mappings)

    def benchmark_construction(self):
        # step 1:
        self.year_page_retrieval()
        # step 2:
        self.pseudo_question_pipeline()
        # step 3:
        self.question_rephrase()

    # stage 1: retrieve all year and month pages for the start and end year range
    def year_page_retrieval(self):
        retrieval = YearPageRetrieval(self.config, self.year_page_out_dir, self.wp_retriever,
                                      self.get_year_month_page_link_pool)
        retrieval.retrieve_page_per_year()
        self.wp_retriever.store_dump()
        self.wp_retriever.annotator.store_cache()

    # stage 2: pipeline for generating pseudo-questions, include:
    # (i) topic entity sampling, (ii) information snippet retrieval and (iii) pseudo-question construction
    def pseudo_question_pipeline(self):
        start_total = time.time()
        # start pipeline for each year range interval. In each interval, repeat the three sub-steps:
        # (i) topic entity sampling, (ii) information snippet retrieval and (iii) pseudo-question construction
        for range in self.year_range_list:
            start = time.time()
            year_start = range[0]
            year_end = range[1]
            self.logger.info(f"Start to generate pseudo-questions for the year range: {range}")
            target_question_number = self.target_question_number_per_range[range]
            self.logger.info(f"The target question number for the year range {range} is: {target_question_number}")
            pseudo_ques_pipeline = PseudoQuestionGeneration(self.config, self.wp_retriever, self.year_page_out_dir,
                                                            year_start, year_end, self.output_dir,
                                                            self.topic_entities_in_total, target_question_number)
            pseudo_ques_pipeline.question_generate_iterative()
            self.pseudo_question_in_total.update(pseudo_ques_pipeline.pseudo_questions)
            self.topic_entities_in_total += pseudo_ques_pipeline.topic_entities
            print("Year start for this year range:", year_start)
            print("Time consumed for this year range:", time.time() - start)

        print("Total time consumed:", time.time() - start_total)
        print("Total number of topic entities:", len(self.pseudo_question_in_total.keys()))
        print("Total number of pseudo questions:",
              sum([len(self.pseudo_question_in_total[topic_entity]) for topic_entity in self.pseudo_question_in_total]))
        with open(self.topic_entities_file_path, "w") as fp:
            for item in list(self.pseudo_question_in_total.keys()):
                fp.write(item)
                fp.write("\n")

        with open(self.pseudo_questions_file_path, "w") as fout:
            fout.write(json.dumps(self.pseudo_question_in_total, indent=4))

        self.wp_retriever.store_dump()
        self.wp_retriever.annotator.store_cache()

    def question_rephrase(self):
        rephrase = PseudoQuestionSampleRephrase(config)
        sample_questions, rephrased_questions, filered_rephrased_questions = rephrase.sample_and_rephrase_pseudo_questions()

        with open(os.path.join(self.output_dir, "sample_questions_in_total.json"), "w") as fp:
            fp.write(json.dumps(sample_questions, indent=4))
        with open(os.path.join(self.output_dir, "rephrased_questions_in_total.json"), "w") as fp:
            fp.write(json.dumps(rephrased_questions, indent=4))
        with open(os.path.join(self.output_dir, "filtered_rephrased_questions_in_total.json"), "w") as fp:
            fp.write(json.dumps(filered_rephrased_questions, indent=4))

    def _year_page_pool(self, year_start, year_end):
        ''' generate year qid and url mappings, for example:

                            "1909": [
                        {
                            "id": "Q2057",
                            "wiki_path": "1909",
                            "label": "1909",
                            "page_type": "year"
                        },
                        {
                            "id": "Q6155680",
                            "wiki_path": "January_1909",
                            "label": "January 1909",
                            "page_type": "event"
                        }]
                            '''
        # map the year range to year
        year_month_pool = {year: [] for year in range(int(year_start), int(year_end) + 1)}
        for year in range(int(year_start), int(year_end) + 1):
            wiki_path = f"{year}"
            # check if year is in wikidata mapping dictionary
            if self.wikidata_mappings.get(wiki_path):
                year_wikidata_id = self.wikidata_mappings.get(wiki_path)
            elif self.wikidata_mappings.get(wiki_path.replace(".", "")):
                year_wikidata_id = self.wikidata_mappings.get(wiki_path.replace(".", ""))
            # get year qid using online service
            else:
                year_wiki_path = f"https://en.wikipedia.org/wiki/{year}"
                try:
                    year_result = get_qid(year_wiki_path)
                    if year_result:
                        year_wikidata_id = year_result[0]
                except:
                    self.logger.info(f"There is no wikidata qid for {year}")
                    continue

            year_month_pool[year].append(
                {'id': year_wikidata_id, 'wiki_path': wiki_path, 'label': str(year), 'page_type': 'year'})

            for month in self.months:
                # There are two kinds of month page url:
                # (1) month_year (2) Portal:Current_events/month_year
                # We take care of them respectively and check which one is effective
                month_wiki_path_1 = f"{month}_{year}"
                month_wiki_path_2 = f"{EVENT_PAGE_PREFIX}/{month_wiki_path_1}"
                if month_wiki_path_1 == "May_2010":
                    # There is an error in the wikidata mapping dictionary
                    # we manually add the link
                    # but this can be removed
                    year_month_pool[year].append(
                        {'id': "Q239311", 'wiki_path': month_wiki_path_2, 'label': f"{month} {year}",
                         'page_type': 'event'})
                    continue
                if self.wikidata_mappings.get(month_wiki_path_2):
                    wikidata_id = self.wikidata_mappings.get(month_wiki_path_2)
                    if wikidata_id != year_wikidata_id:
                        year_month_pool[year].append(
                            {'id': wikidata_id, 'wiki_path': month_wiki_path_2, 'label': f"{month} {year}",
                             'page_type': 'event'})
                elif self.wikidata_mappings.get(month_wiki_path_1):
                    wikidata_id = self.wikidata_mappings.get(month_wiki_path_1)
                    if wikidata_id != year_wikidata_id:
                        year_month_pool[year].append(
                            {'id': wikidata_id, 'wiki_path': month_wiki_path_1, 'label': f"{month} {year}",
                             'page_type': 'event'})
                    elif year >= 2021:
                        # when year is greater than 2021, the url changes to another format
                        wikipedia_link = f"https://en.wikipedia.org/wiki/{month_wiki_path_2}"
                        result = get_qid(wikipedia_link)
                        if result and result[0] != year_wikidata_id:
                            wikidata_id = result[0]
                            year_month_pool[year].append(
                                {'id': wikidata_id, 'wiki_path': month_wiki_path_2, 'label': f"{month} {year}",
                                 'page_type': 'event'})
                        else:
                            wikipedia_link = f"https://en.wikipedia.org/wiki/{month_wiki_path_1}"
                            result = get_qid(wikipedia_link)
                            if result and result[0] != year_wikidata_id:
                                wikidata_id = result[0]
                                year_month_pool[year].append(
                                    {'id': wikidata_id, 'wiki_path': month_wiki_path_1, 'label': f"{month} {year}",
                                     'page_type': 'event'})

        return year_month_pool


#######################################################################################################################
#######################################################################################################################
if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception(
            "Usage: python tiq/pipeline.py <FUNCTION> <PATH_TO_CONFIG>"
        )

    # load config
    function = sys.argv[1]
    config_path = sys.argv[2]
    config = get_config(config_path)

    if function == "--year-page-retrieve":
        benchmark = Pipeline(config)
        benchmark.year_page_retrieval()

    elif function == "--pseudoquestion-generate":
        benchmark = Pipeline(config)
        benchmark.pseudo_question_pipeline()

    elif function == "--question-rephrase":
        benchmark = Pipeline(config)
        benchmark.question_rephrase()

    else:
        raise Exception(f"Unknown function {function}!")
