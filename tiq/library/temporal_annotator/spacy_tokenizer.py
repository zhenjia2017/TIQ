"""Tokenizer that is backed by spaCy (spacy.io).
Requires spaCy package and the spaCy english model.
"""

import spacy

from tiq.library.temporal_annotator.tokenizer import Tokens, Tokenizer


class SpacyTokenizer(Tokenizer):
    def __init__(self, config):
        self.model = config["spacy_model"]
        self.nlp = spacy.load(self.model)

    def tokenize(self, text):
        # We don't treat new lines as tokens.
        clean_text = text.replace('\n', ' ')
        tokens = self.nlp(clean_text)

        data = []
        for i in range(len(tokens)):
            # Get whitespace
            start_ws = tokens[i].idx
            if i + 1 < len(tokens):
                end_ws = tokens[i + 1].idx
            else:
                end_ws = tokens[i].idx + len(tokens[i].text)

            data.append((
                tokens[i].text,
                text[start_ws: end_ws],
                (tokens[i].idx, tokens[i].idx + len(tokens[i].text)),
                tokens[i].pos_,
                tokens[i].tag_,
                tokens[i].lemma_,
                tokens[i].ent_type_,
            ))

        # Set special option for non-entity tag: '' vs 'O' in spaCy
        return Tokens(data, opts={'non_ent': ''})
