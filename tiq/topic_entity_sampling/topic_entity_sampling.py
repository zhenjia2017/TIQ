import json
import os
import random

from tqdm import tqdm

from tiq.library.utils import get_logger


class TopicEntitySampling:
    def __init__(self, config, output_dir, year_page_out_dir, year_start, year_end, sampled_topic_entities):
        """Create the pipeline based on the config."""
        # load config
        self.config = config
        self.logger = get_logger(__name__, config)

        self.data_path = self.config["data_path"]
        # choosing the boundaries to overlap (e.g., 10 as default) years for each interval as a temporal constraint.
        # for example: 1801-1860, 1851-1910 etc.
        # reason for this: for each interval, you still go only for events in the first 50 years,
        # but by considering the subsequent 10 years you may avoid that you lose
        # the reference for the temporal constraint (e.g., ... before ... where the first part is in 1801-1850 but the second part is in 1855 or so.)
        self.overlap_years = self.config["overlap_years_num"]
        self.year_page_out_dir = year_page_out_dir
        self.sample_size = self.config["sample_size"]
        self.long_tail_entity_frequency = self.config["long_tail_entity_frequency"]
        self.prominent_entity_frequency = self.config["prominent_entity_frequency"]
        self.ratio_of_sample = self.config["ratio_of_sample"]

        self.domain_coverage = self.config["domain_coverage"]
        # already sampled entity from the pool
        self.sampled_topic_entities = sampled_topic_entities
        self.output_dir = output_dir

        # year page range for retrieval
        self.year_start = year_start
        self.year_end = year_end
        self.year_evidence_file = os.path.join(self.output_dir, f'yearpages.jsonl')
        self.year_pages_entities_info_dump = os.path.join(self.output_dir,
                                                          f'yearpages_entity_label_type_frequency.json')

        self.year_entity_info = self.merge_year_page(self.year_evidence_file, self.year_pages_entities_info_dump)

        print("length of entity pool for sampling:", str(len(self.year_entity_info)))
        # split entities in year pages into three sets
        self.long_tail_entities, self.prominent_entities, self.other_entities = self.split_entities(
            self.long_tail_entity_frequency, self.prominent_entity_frequency)

    def merge_year_page(self, year_evidence_file, year_pages_entities_info_dump):
        file_list = os.listdir(self.year_page_out_dir)

        year_evidence_entity_pages = {}
        for file in file_list:
            year = int(file.split("_")[0])
            if year not in year_evidence_entity_pages:
                year_evidence_entity_pages[year] = []
            year_evidence_entity_pages[year].append(file)

        # merge year page evidences as constraint
        with open(year_evidence_file, "w") as fin:
            year_range = []
            for year in range(self.year_start, self.year_end + 1 + self.overlap_years):
                if year in year_evidence_entity_pages:
                    # we use overlap year evidences as constraint: 1801-1860, 1851-1910
                    files = year_evidence_entity_pages[year]
                    year_range.append(year)
                    for file in files:
                        if file.endswith(".jsonl"):
                            with open(os.path.join(self.year_page_out_dir, file), "r") as fp:
                                for line in tqdm(fp):
                                    evidence = json.loads(line)
                                    fin.write(json.dumps(evidence))
                                    fin.write("\n")

            self.logger.info(f"year range as temporal constraint: {year_range}")

        # merge entities in year page for sampling pool
        year_range = []
        year_entity_info = []
        for year in range(self.year_start, self.year_end + 1):
            # don't overlap years for entity pool
            files = year_evidence_entity_pages[year]
            year_range.append(year)
            for file in files:
                if file.endswith(".json"):
                    with open(os.path.join(self.year_page_out_dir, file), 'r') as fin:
                        data = json.load(fin)
                        for item in data:
                            if item not in year_entity_info:
                                year_entity_info.append(item)

        self.logger.info(f"year range as temporal constraint and for entity samping: {year_range}")
        self.logger.info(f"Number of entities for samping: {len(year_entity_info)}")
        # sort year entity
        entity_info_sort = sorted(year_entity_info, key=lambda x: x['frequency'], reverse=True)
        with open(year_pages_entities_info_dump, 'w') as fp:
            fp.write(json.dumps(entity_info_sort, indent=4))

        return entity_info_sort

    def split_entities(self, long_tail_entity_threshold, prominent_entity_threshold):
        # split entities into three sets according to their frequency
        long_tail_entities = []
        prominent_entities = []
        other_entities = []
        for entity in tqdm(self.year_entity_info):
            if entity["frequency"] < long_tail_entity_threshold:
                long_tail_entities.append(entity)
            elif entity["frequency"] > prominent_entity_threshold:
                prominent_entities.append(entity)
            else:
                other_entities.append(entity)
        return long_tail_entities, prominent_entities, other_entities

    def randomly_sample_entity(self, entity_pool):
        return random.sample(entity_pool, 1)

    def sample_from_types(self, entity_pool, SAMPLE_SIZE=50):
        type_frequency_dict = dict()
        random_entity_sample = list()
        type_distribution = float(
            self.domain_coverage)  # individual entity types are not taking up more than 10% of the topic entities
        for i in range(SAMPLE_SIZE):
            entity = self.randomly_sample_entity(entity_pool)
            entity_id = entity[0]["id"]
            entity_label = entity[0]["label"]
            entity_frequency = entity[0]["frequency"]
            entity_types = entity[0]["type"]
            types = [item["label"] for item in entity_types]
            if not types:
                continue
            skip_entity = False
            for type in types:
                if type == "human" or type == "Q5":
                    continue
                if type not in type_frequency_dict:
                    type_frequency_dict[type] = 0

                if type_frequency_dict[type] > int(type_distribution * SAMPLE_SIZE):
                    skip_entity = True
                else:
                    type_frequency_dict[type] += 1

            if skip_entity:
                continue

            retrieve_for_entity = {"id": entity_id, "label": entity_label, "type": types, "frequency": entity_frequency}
            if retrieve_for_entity not in random_entity_sample:
                random_entity_sample.append(retrieve_for_entity)

        return random_entity_sample

    def sample_entity_for_retrieval(self):
        # sample from long-tail, prominent, and other entities according to the ratio
        portion = []
        sampled_entity = {}
        if "long" in self.ratio_of_sample:
            long_ratio = float(self.ratio_of_sample["long"])
            portion.append(long_ratio)
        if "prominent" in self.ratio_of_sample:
            prominent_ratio = float(self.ratio_of_sample["prominent"])
            portion.append(prominent_ratio)
        if "other" in self.ratio_of_sample:
            other_ratio = float(self.ratio_of_sample["other"])
            portion.append(other_ratio)

        if "long" in self.ratio_of_sample:
            long_sample_portions = int(self.sample_size / sum(portion) * long_ratio)
            self.long_tail_entities_for_sample = [item for item in self.long_tail_entities if
                                                  item["id"] not in self.sampled_topic_entities]
            sample_long_tail_entity = self.sample_from_types(self.long_tail_entities_for_sample,
                                                             min(len(self.long_tail_entities_for_sample),
                                                                 long_sample_portions))
            sampled_entity["long"] = sample_long_tail_entity
            self.logger.info(f"number of long tail entities: {len(sample_long_tail_entity)}")
        if "prominent" in self.ratio_of_sample:
            prominent_sample_portions = int(self.sample_size / sum(portion) * prominent_ratio)
            self.prominent_entities_for_sample = [item for item in self.prominent_entities if
                                                  item["id"] not in self.sampled_topic_entities]
            sample_prominent_entity = self.sample_from_types(self.prominent_entities_for_sample,
                                                             min(len(self.prominent_entities_for_sample),
                                                                 prominent_sample_portions))
            sampled_entity["prominent"] = sample_prominent_entity
            self.logger.info(f"number of prominent entities: {len(sample_prominent_entity)}")
        if "other" in self.ratio_of_sample:
            other_sample_portions = int(self.sample_size / sum(portion) * other_ratio)
            self.other_entities_for_sample = [item for item in self.other_entities if
                                              item["id"] not in self.sampled_topic_entities]
            sampled_other_entity = self.sample_from_types(self.other_entities_for_sample,
                                                          min(len(self.other_entities_for_sample),
                                                              other_sample_portions))
            sampled_entity["other"] = sampled_other_entity
            self.logger.info(f"number of other entities: {len(sampled_other_entity)}")

        return sampled_entity
