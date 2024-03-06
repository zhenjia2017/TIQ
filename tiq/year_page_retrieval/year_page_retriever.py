import json
import os
import re
import time

from tiq.library.utils import get_logger
# year page retrieval
from tiq.year_page_retrieval.year_event_retriever import YearEventRetriever

ENT_PATTERN = re.compile("^Q[0-9]+$")


class YearPageRetrieval:
    def __init__(self, config, year_page_out_dir, wp_retriever, year_month_page_link_pool):
        """Create the pipeline based on the config."""
        # load config
        self.config = config
        self.logger = get_logger(__name__, config)

        self.data_path = self.config["data_path"]
        self.year_page_out_dir = year_page_out_dir
        self.year_start = self.config["year_start"]
        self.year_end = self.config["year_end"]
        self.year_month_page_link_pool = year_month_page_link_pool

        self.wp_retriever = wp_retriever
        self.clocq = self.wp_retriever.clocq
        self.year_retriever = YearEventRetriever(config, self.wp_retriever)

    def retrieve_page_per_year(self):
        # For the urls pool, retrieve the information from the text. Note that for each year, there can be multiple urls including months in the year.
        # The information is stored in the year granularity. All months of a year information is stored in the year json file.
        for year in range(self.year_start, self.year_end + 1):
            self.year_range_pages_per_year = self.year_month_page_link_pool[year]
            self.year_evidence_file = os.path.join(self.year_page_out_dir, f'{year}_yearpages.jsonl')
            self.year_pages_entities_info_dump = os.path.join(self.year_page_out_dir,
                                                              f'{year}_yearpages_entity_label_type_frequency.json')

            # skip the retrieval if the file already exists
            if os.path.exists(self.year_evidence_file) and os.path.exists(self.year_pages_entities_info_dump):
                self.logger.info(f"year information exist {year}")
                continue

            entity_info_sort = self.retrieve_year_page(self.year_range_pages_per_year, self.year_evidence_file,
                                                       self.year_pages_entities_info_dump)
            self.logger.info(f"length of entity pool for sampling: {str(len(entity_info_sort))}")

    def retrieve_year_page(self, range_pages, year_evidence_file, year_pages_entities_info_dump):
        # retrieve year pages' texts
        start = time.time()
        evidences, entities = self.year_retriever.year_retriever(range_pages)
        if evidences:
            self.logger.info(f"length of retrieved evidences: {str(len(evidences))}")
            with open(year_evidence_file, 'w') as fp:
                for evidence in evidences:
                    fp.write(json.dumps(evidence))
                    fp.write("\n")

        # annotate wikipedia evidence
        entity_info_sort = self._year_page_entity_info(entities)
        # store annotated information snippets
        with open(year_pages_entities_info_dump, "w") as fp:
            fp.write(json.dumps(entity_info_sort, indent=4))

        self.logger.info(f"Retrieval time consumed for each year: {time.time() - start}")
        return entity_info_sort

    def _year_page_entity_info(self, year_page_entities):
        # information of entities in year pages: qid, label, types, and frequency
        entities = []
        entity_info = []
        for item in year_page_entities:
            if "id" in item:
                if ENT_PATTERN.match(item["id"]) and item not in entities:
                    # frequency of an entity
                    frequency = sum(self.clocq.get_frequency(item["id"]))
                    label = item["label"]
                    types = self.clocq.get_types(item["id"])
                    # entity should have types
                    if types:
                        info = {"id": item["id"], "label": label, "type": types, "frequency": frequency}
                        entity_info.append(info)
                        entities.append(item)
            else:
                self.logger.info(f"Entity has no id!!: {item}")
                continue

        print("Total entities in year pages: ")
        print(len(entities))
        entity_info_sort = sorted(entity_info, key=lambda x: x['frequency'], reverse=True)
        return entity_info_sort
