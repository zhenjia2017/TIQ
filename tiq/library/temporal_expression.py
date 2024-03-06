import os

from spacy.matcher import Matcher
from spacy.tokens import Span

from tiq.library.temporal_library import TemporalValueAnnotator
from tiq.library.utils import get_logger


class TemporalAnnotator:
    # temporal annotation result
    def __init__(self, d):
        self.id = d['id']
        # annotation type: date, explicit signal, implicit signal, ordinal
        self.type = d['type']
        # expression text
        self.mention = d['mention']
        # expression text
        self.signal = d['signal']
        # expression mention text span
        self.span = d['span']
        # date, time span, ordinal number
        self.value = d['value']
        # date: sutime or regex; explicit: spacy; implicit: spacy; ordina: regex
        self.method = d['method']
        # hierarchical nested annotation
        self.nest = d['nest']

    def json_dict(self):
        # Simple dictionary representation
        return {'id': self.id,
                'type': self.type,
                'mention': self.mention,
                'signal': self.signal,
                'value': self.value,
                'span': self.span,
                'method': self.method,
                'nest': self.nest
                }


class ExplicitExpression:
    # An explicit expression
    def __init__(self, d):
        # signal
        self.signal = d['signal']
        # normalized time span
        self.timespan = d['timespan']
        # expression text
        self.text = d['text']
        # expression mention text span
        self.span = d['span']
        self.signal_span = d['signal_span']
        self.signal_text = d['signal_text']
        # expression mention text length
        self.length = d['length']
        # hierarchical nested annotation
        self.nest = d['nest']

    def json_dict(self):
        # Simple dictionary representation
        return {'text': self.text,
                'signal': self.signal,
                'timespan': self.timespan,
                'span': self.span,
                'signal_span': self.signal_span,
                'signal_text': self.signal_text,
                'length': self.length,
                'nest': self.nest
                }


# Read signal keywords from signal_keywords.txt and generate explicit expression rules
class TemporalExpression:
    def __init__(self, config):
        self.config = config
        self.logger = get_logger(__name__, config)
        self.pattern = dict()
        self.temporal_value_annotator = TemporalValueAnnotator(config)
        self.date_ordinal_annotator = self.temporal_value_annotator.date_ordinal_annotator
        self.tokenizer = self.temporal_value_annotator.tokenizer
        self.explicit_signal_type = dict()
        # open explicit signal keywords file
        with open(os.path.join(self.config["data_path"], self.config["path_to_explicit_signals"]), "r") as fp:
            for line in fp.readlines():
                keyword = line.split("||")[0].strip()
                signal = line.split("||")[1].strip()
                if signal not in self.explicit_signal_type:
                    self.explicit_signal_type[signal] = set()
                self.explicit_signal_type[signal].add(keyword)

        self.explicit_patterns = self._explicit_pattern()

    def _explicit_pattern(self):
        pattern = {}
        for signal, keywords in self.explicit_signal_type.items():
            pattern[signal] = []
            single_word = []
            multiple_word = []
            for keyword in keywords:
                if " " not in keyword:
                    single_word.append(keyword)
                else:
                    multiple_word.append(keyword)
            if signal == "DURATION":
                pattern[signal].append(
                    [{"ENT_TYPE": "TEMP", "OP": "+"}, {"LEMMA": {"IN": single_word}}, {"ENT_TYPE": "TEMP", "OP": "+"}])
                for word in multiple_word:
                    multiple_word_pattern = []
                    word_list = word.split(" ")
                    multiple_word_pattern.append({"ENT_TYPE": "TEMP", "OP": "+"})
                    for i in range(0, len(word_list)):
                        multiple_word_pattern.append({"LEMMA": word_list[i]})
                    multiple_word_pattern.append({"ENT_TYPE": "TEMP", "OP": "+"})
                    pattern[signal].append(multiple_word_pattern)
            else:
                pattern[signal].append([{"LEMMA": {"IN": single_word}}, {"ENT_TYPE": "TEMP", "OP": "+"}])
                for word in multiple_word:
                    multiple_word_pattern = []
                    word_list = word.split(" ")
                    for i in range(0, len(word_list)):
                        multiple_word_pattern.append({"LEMMA": word_list[i]})
                    multiple_word_pattern.append({"ENT_TYPE": "TEMP", "OP": "+"})
                    pattern[signal].append(multiple_word_pattern)
        return pattern

    def exlicit_pattern_match(self, string, doc, signal, pattern, disambiguations):
        matcher = Matcher(self.tokenizer.nlp.vocab)
        matcher.add(signal, pattern)
        matches = matcher(doc)
        matches.sort(key=lambda x: x[1])
        matched_expressions = []
        if len(matches) > 0:
            for match_id, token_start, token_end in matches:
                # Create the matched span and assign the match_id as a label
                span = Span(doc, token_start, token_end, label=match_id)
                expression_start = string.index(span.text)
                expression_end = expression_start + len(span.text)
                timespan = []
                for date_span in disambiguations.keys():
                    start_char = date_span[0]
                    end_char = date_span[1]
                    if start_char >= expression_start and end_char <= expression_end:
                        expression_length = expression_end - expression_start
                        signal_word_start = expression_start
                        signal_word_end = start_char - 1
                        if signal == 'DURATION':
                            timespan.append((disambiguations[date_span][1], date_span))
                        else:
                            matched_expressions.append(ExplicitExpression(
                                {'text': span.text, 'span': (expression_start, expression_end),
                                 'signal_span': (signal_word_start, signal_word_end),
                                 'signal_text': string[signal_word_start:signal_word_end],
                                 'timespan': disambiguations[date_span][1],
                                 'signal': signal,
                                 'nest': [date_span],
                                 'length': expression_length}))

                if signal == 'DURATION' and len(timespan) > 0:
                    range = {}
                    for item in timespan:
                        try:
                            range[item[0][0]] = int(item[0][0].replace('-', '').replace('T00:00:00Z', ''))
                            range[item[0][1]] = int(item[0][1].replace('-', '').replace('T00:00:00Z', ''))
                        except:
                            print("Annotation error: ", item)
                            continue

                    time_range = [k for k, v in sorted(range.items(), key=lambda item: item[1])]
                    if len(time_range) > 0:
                        matched_expressions.append(ExplicitExpression(
                            {'text': span.text, 'span': (expression_start, expression_end),
                             'signal_span': ('', ''),
                             'signal_text': '',
                             'timespan': (time_range[0], time_range[-1]),
                             'signal': signal,
                             'nest': [item[1] for item in timespan],
                             'length': expression_length}))

        return [w.json_dict() for w in matched_expressions]

    def remove_duplicate_matched_expression(self, disambiguations, signal_expression):
        date_in_matched_pattern = {}
        for date_span in disambiguations.keys():
            matched_patterns = []
            start_char = date_span[0]
            end_char = date_span[1]
            for signal, matchs in signal_expression.items():
                for match in matchs:
                    if start_char >= match['span'][0] and end_char <= match['span'][1]:
                        # if a date in a matched expression
                        matched_patterns.append(match)
                        # if signal is DURATION, keep the expression
                        if signal == 'DURATION':
                            date_in_matched_pattern[date_span] = match
            if len(matched_patterns) == 1 and date_span not in date_in_matched_pattern:
                date_in_matched_pattern[date_span] = matched_patterns[0]
            # if a date in multiple matched expression, keep the shortest expression
            elif len(matched_patterns) > 1 and date_span not in date_in_matched_pattern:
                date_in_matched_pattern[date_span] = sorted(matched_patterns, key=lambda d: d['length'])[0]
            elif len(matched_patterns) == 0:
                # disambiguations.update({item['span']: [text, timespan]})
                date_in_matched_pattern[date_span] = ExplicitExpression(
                    {'text': disambiguations[date_span][0], 'span': (start_char, end_char),
                     'timespan': disambiguations[date_span][1],
                     'signal': 'No signal',
                     'signal_span': ('', ''),
                     'signal_text': '',
                     'length': end_char - start_char,
                     'nest': [date_span]}).json_dict()
        return list(date_in_matched_pattern.values())

    def annotateExplicitTemporalExpressions(self, string, reference_time, date_tag_method):
        temporal_annotations = self.date_ordinal_annotator(string, reference_time, date_tag_method)
        date_annotator_result = temporal_annotations[0]
        ordinal_annotator_result = temporal_annotations[1]
        disambiguations = {}
        for item in date_annotator_result:
            text = item['text']
            timespan = item['timespan']
            disambiguations.update({item['span']: [text, timespan]})
        spans = []
        doc = self.tokenizer.nlp(string)
        # list of spans for date as entity
        for item in list(disambiguations.keys()):
            # add the tag "TEMP" as a new entity label for spacy
            date_span = doc.char_span(item[0], item[1], label="TEMP")
            if date_span:
                spans.append(date_span)
            # annotate date as entities in text
        doc.set_ents(entities=spans)

        explicit_signal_expression = {}
        for signal, pattern in self.explicit_patterns.items():
            # print (pattern)
            explicit_signal_expression[signal] = self.exlicit_pattern_match(string, doc, signal, pattern,
                                                                            disambiguations)

        # remove duplicated explicit expressions
        explicit_signals = self.remove_duplicate_matched_expression(disambiguations, explicit_signal_expression)

        annotation_result = []
        id = 0
        nest_relation = {}
        for item in date_annotator_result:
            type = 'date'
            text = item['text']
            method = item['method']
            value = {'timespan': item['timespan'], 'disambiguation': item['disambiguation']}
            span = item['span']
            signal = ''
            annotation_result.append(TemporalAnnotator(
                {'id': id, 'type': type, 'mention': text, 'value': value, 'span': span, 'method': method,
                 'signal': signal, 'nest': []}))
            nest_relation[span] = id
            id += 1

        for item in ordinal_annotator_result:
            type = 'ordinal'
            text = item['text']
            method = 'regex'
            value = item['ordinal']
            span = item['span']
            signal = 'ORDINAL'
            annotation_result.append(TemporalAnnotator(
                {'id': id, 'type': type, 'mention': text, 'value': value, 'span': span, 'method': method,
                 'signal': signal, 'nest': []}))
            nest_relation[span] = id
            id += 1

        for item in explicit_signals:
            type = 'explicit'
            text = item['text']
            method = 'regexSpacy'
            value = item['timespan']
            span = item['span']
            signal = item['signal']
            nest = [nest_relation[nest_span] for nest_span in item['nest']]
            annotation_result.append(TemporalAnnotator(
                {'id': id, 'type': type, 'mention': text, 'value': value, 'span': span, 'method': method,
                 'signal': signal, 'nest': nest}))
            nest_relation[span] = id
            id += 1

        return [w.json_dict() for w in
                annotation_result], explicit_signals, date_annotator_result, ordinal_annotator_result
