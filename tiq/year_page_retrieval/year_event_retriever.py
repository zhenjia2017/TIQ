import re
import time

import nltk

from tiq.library.utils import get_logger, format_text

EVENT_PAGE_PREFIX = "Portal:Current_events"


class YearEventRetriever:
    def __init__(self, config, wp_retriever):
        self.config = config
        self.logger = get_logger(__name__, config)
        self.config = config
        self.logger = get_logger(__name__, config)
        self.wp_retriever = wp_retriever

    def year_retriever(self, year_range_pages):
        year_evidences = []
        year_page_entities = []
        for page in year_range_pages:
            evidences, entities = self.retrieve_year_evidences_from_page(page)
            self.logger.info(f"Length of evidences : {page}: {len(evidences)}")
            year_evidences += evidences
            year_page_entities += entities
        print("length of evidences:", str(len(year_evidences)))
        print("length of entities:", str(len(year_page_entities)))
        return year_evidences, year_page_entities

    def remove_noise_char_from_text(self, text):
        text = re.sub(r"\[[0-9]*\]", "", text)
        text = text.replace("( )", " ")
        text = text.replace("( % )", " ")
        text = text.replace(", ", " ")
        text = text.replace("•", " ")
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
        words = nltk.word_tokenize(new_text)
        new_words = " ".join(words)
        cleaned_text = self.remove_noise_char_from_text(new_words)
        cleaned_text = cleaned_text.encode('utf-8').decode('utf-8')
        cleaned_text = format_text(cleaned_text)
        cleaned_text = f"{cleaned_text}."
        return cleaned_text

    def wiki_evidence_to_template(self, evidence):
        # ["tempinfo"] = [timespans, timedisambiguations, dates, timetexts, timepositions]
        timespans, timedisambiguations, dates, timetexts, timepositions = evidence["tempinfo"]
        text = evidence['evidence_text']
        if len(timespans) == 1:
            position = timepositions[0]
            start_pos = int(position[0])
            end_pos = int(position[1])
            text = text[:start_pos] + text[end_pos:]
        elif len(timespans) == 2:
            date_positions_in_text = [(int(item[0]), int(item[1])) for item in timepositions]
            # there are duplicated expressions with same dates
            # remove dates from back to front
            sorted_data = sorted(date_positions_in_text, key=lambda x: list(x)[1], reverse=True)
            for item in sorted_data:
                start_pos = item[0]
                end_pos = item[1]
                text = text[:start_pos] + text[end_pos:]

        cleaned_text = self.clean_text(text)
        evidence['candidate_question_text'] = cleaned_text
        disambiguations = {item[1]: item[0] for item in evidence["disambiguations"]}
        for item in evidence["wikidata_entities"]:
            item.update({"disam_label": disambiguations[item["id"]]})
        date_entities = []
        non_date_entities = []

        for item in evidence["wikidata_entities"]:
            if "T00:00:00Z" in item["id"]:
                date_entities.append(item)
            else:
                non_date_entities.append(item)

        if len(non_date_entities) >= 1:
            evidence["question_entity"] = non_date_entities
            evidence["answer_entity"] = []

    def retrieve_year_evidences_from_page(self, year_page):
        """
        Retrieve texts from wikipedia
        """
        self.logger.info(f"Retrieve evidences for: {year_page}")
        # first entities (if required)
        start = time.time()
        evidences = []
        if year_page["page_type"] == "year":
            wiki_evidences, entities = self.wp_retriever.wp_year_retriever(year_page)

            for evidence in wiki_evidences:
                self.wiki_evidence_to_template(evidence)
                evidences.append(evidence)

            self.logger.info(
                f"Time taken (retrieve_wikipedia_evidences): {time.time() - start} seconds")

            return evidences, entities
        # {'id': wikidata_id, 'wiki_path':month_wiki_path, 'label': month_wiki_label}
        else:
            # In the event page (page_type = "event"), we only extract the entities in the page
            # for enlarging the entities pool
            entities = self.wp_retriever.wp_event_retriever(year_page)
            self.logger.info(
                f"Time taken (retrieve_wikipedia_evidences): {time.time() - start} seconds")

            return [], entities
