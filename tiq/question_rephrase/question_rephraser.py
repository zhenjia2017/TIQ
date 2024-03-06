import os
import pickle
import time
from pathlib import Path

import openai
from filelock import FileLock

from tiq.library.utils import get_logger, format_text

REPHRASE_PROMPT = """Please rephrase the following input question into a more natural question.

Input: What album Sting ( musician ) was released, during, Sting award received German Radio Award?
Question: which album was released by Sting when he won the German Radio Award?

Input: What human President of Bolivia was the second and most recent female president, after, president of Bolivia officeholder Evo Morales?
Question: Which female president succeeded Evo Morales in Bolivia?

Input: What lake David Bowie He moved to Switzerland purchasing a chalet in the hills to the north of , during, David Bowie spouse Angela Bowie?
Question: Close to which lake did David Bowie buy a chalet while he was married to Angela Bowie?

Input: What human Robert Motherwell spouse, during, Robert Motherwell He also edited Paalen 's collected essays Form and Sense as the first issue of Problems of Contemporary Art?
Question: Who was Robert Motherwell's wife when he edited Paalen's collected essays Form and Sense?

Input: What historical country Independent State of Croatia the NDH government signed an agreement with which demarcated their borders, during, Independent State of Croatia?
Question: At the time of the Independent State of Croatia, which country signed an agreement with the NDH government to demarcate their borders?

Input: What U-boat flotilla German submarine U-559 part of, before, German submarine U-559 She moved to the 29th U-boat Flotilla?
Question: Which U-boat flotilla did the German submarine U-559 belong to before being transferred to the 29th U-boat Flotilla?

Input: What human UEFA chairperson, during, UEFA chairperson Sandor Barcs?
Question: Who was the UEFA chairperson after Sandor Barcs?

Input: What human Netherlands head of government, during, Netherlands head of state Juliana of the Netherlands?
Question: During Juliana of the Netherlands' time as queen, who was the prime minister in the Netherlands?

Input: """


class QuestionRephrase:
    def __init__(self, config):
        # load or generate frequency for each qid
        self.logger = get_logger(__name__, config)
        self.config = config
        self.model = self.config["gpt3_model"]
        self.cache_dir = os.path.join(self.config["data_path"], self.config["gpt3_cache_path"])
        self.cache_dir = Path(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = os.path.join(self.cache_dir, "gpt_cache.pickle")
        self.gpt_output = dict()
        # initialize gpt3 output cache dictionary
        self.use_cache = self.config["gpt3_use_cache"]
        if self.use_cache:
            self._init_cache()
            self.cache_changed = False

    def check_signal_same(self, rephrased_question, signal):
        if signal == "OVERLAP":
            if "after" in rephrased_question.lower() or "before" in rephrased_question.lower():
                return False
            else:
                return True
        elif signal == "BEFORE" or signal == "AFTER":
            if "during" in rephrased_question.lower():
                return False
            elif "while" in rephrased_question.lower():
                return False
            elif "in the year" in rephrased_question.lower():
                return False
            elif "when" in rephrased_question.lower():
                return False
            elif "at the time" in rephrased_question.lower():
                return False
            else:
                return True

    def rephrase_question(self, entity_statement_map):
        for entity in entity_statement_map:
            for example in entity_statement_map[entity]:
                pseudo_question = example["pseudo_question_construction"]
                pseudo_question = format_text(pseudo_question).encode('utf-8').decode('utf-8')
                rephrased_question = self.gpt_rephrase_question(pseudo_question)
                rephrased_question_length = len(rephrased_question.split())
                self.logger.info(f"generate question: {rephrased_question}")
                if rephrased_question:
                    example["rephrased_question"] = rephrased_question
                    example["rephrased_question_length"] = rephrased_question_length
                    example["signal_check"] = self.check_signal_same(rephrased_question, example["signal"])

    def rephrase_on_instance(self, instance):
        pseudo_question = instance["pseudo_question_construction"]
        signal = instance["signal"]
        pseudo_question = format_text(pseudo_question).encode('utf-8').decode('utf-8')
        rephrased_question = self.gpt_rephrase_question(pseudo_question)
        if rephrased_question:
            self.logger.info(f"generate question: {rephrased_question}")
            rephrased_question_length = len(rephrased_question.split())
            return [rephrased_question, rephrased_question_length, self.check_signal_same(rephrased_question, signal)]

    def gpt_rephrase_question(self, statement):
        print("Question:", statement)
        question_prompt = REPHRASE_PROMPT + statement + "\nQuestion:"
        generated_question = None

        if question_prompt in self.gpt_output:
            return self.gpt_output[question_prompt]

        if self.use_cache and question_prompt in self.cache:
            generated_question = self.cache[question_prompt]
            self.gpt_output.update({question_prompt: generated_question})
            self.logger.info(f"From cache {generated_question}")
        else:
            # generate output
            if self.model == "gpt-3.5-turbo":
                generated_question = self._prompt_chat_gpt(question_prompt)
            elif self.model == "text-davinci-003":
                generated_question = self._prompt_instruct_gpt(question_prompt)

            if generated_question:
                self.gpt_output.update({question_prompt: generated_question})
                if self.use_cache:
                    self.cache_changed = True
                    self.cache[question_prompt] = generated_question
                # self.logger.info(f"Generate istf for question {question}")
                self.logger.info(f"From openai service {generated_question}")
        return generated_question

    def _prompt_chat_gpt(self, question_prompt):
        openai.organization = self.config["openai_organization"]
        openai.api_key = self.config["openai_api_key"]
        ## WITH CHAT GPT
        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[{"role": "user", "content": question_prompt}]
            )
            generated_question = response.choices[0].message.content.strip()
            self.cache[question_prompt] = generated_question
            return generated_question
        except:
            print(f"FAIL: GPT did not respond for the following question_prompt")

    def _prompt_instruct_gpt(self, question_prompt):
        ## WITH INSTRUCT GPT
        openai.organization = self.config["openai_organization"]
        openai.api_key = self.config["openai_api_key"]
        try:
            response = openai.Completion.create(
                model=self.model,
                prompt=question_prompt,
                temperature=1.0,
                max_tokens=256,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )
            generated_question = response["choices"][0]["text"].strip()
            self.cache[question_prompt] = generated_question
            return generated_question
        except:
            print(f"FAIL: GPT did not respond for the following question_prompt")

    def store_cache(self):
        """Store the cache to disk."""
        if not self.use_cache:  # store only if cache in use
            return
        if not self.cache_changed:  # store only if cache changed
            return
        # check if the cache was updated by other processes
        if self._read_cache_version() == self.cache_version:
            # no updates: store and update version
            self.logger.info(f"Writing TVR GPT3 cache at path {self.cache_path}.")
            with FileLock(f"{self.cache_path}.lock"):
                self._write_cache(self.cache)
                self._write_cache_version()
        else:
            # update! read updated version and merge the caches
            self.logger.info(f"Merging TVR GPT3 cache at path {self.cache_path}.")
            with FileLock(f"{self.cache_path}.lock"):
                # read updated version
                updated_cache = self._read_cache()
                # overwrite with changes in current process (most recent)
                updated_cache.update(self.cache)
                # store
                self._write_cache(updated_cache)
                self._write_cache_version()

    def _init_cache(self):
        """Initialize the cache."""
        if os.path.isfile(self.cache_path):
            # remember version read initially
            self.logger.info(f"Loading TVR GPT3 cache from path {self.cache_path}.")
            with FileLock(f"{self.cache_path}.lock"):
                self.cache_version = self._read_cache_version()
                self.logger.debug(self.cache_version)
                self.cache = self._read_cache()
            self.logger.info(f"TVR GPT3 cache successfully loaded.")
        else:
            self.logger.info(f"Could not find an existing TVR GPT3 cache at path {self.cache_path}.")
            self.logger.info("Populating TVR GPT3 cache from scratch!")
            self.cache = {}
            self._write_cache(self.cache)
            self._write_cache_version()

    def _read_cache(self):
        """
        Read the current version of the cache.
        This can be different from the version used in this file,
        given that multiple processes may access it simultaneously.
        """
        # read file content from cache shared across QU methods
        with open(self.cache_path, "rb") as fp:
            cache = pickle.load(fp)
        return cache

    def _write_cache(self, cache):
        """Write to the cache."""
        cache_dir = os.path.dirname(self.cache_path)
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "wb") as fp:
            pickle.dump(cache, fp)
        return cache

    def _read_cache_version(self):
        """Read the cache version (hashed timestamp of last update) from a dedicated file."""
        if not os.path.isfile(f"{self.cache_path}.version"):
            self._write_cache_version()
        with open(f"{self.cache_path}.version", "r") as fp:
            cache_version = fp.readline().strip()
        return cache_version

    def _write_cache_version(self):
        """Write the current cache version (hashed timestamp of current update)."""
        with open(f"{self.cache_path}.version", "w") as fp:
            version = str(time.time())
            fp.write(version)
        self.cache_version = version
