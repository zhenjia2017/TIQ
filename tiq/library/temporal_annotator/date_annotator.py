# from sutime import SUTime
import re
from concurrent.futures import ThreadPoolExecutor

REGEX_NUM_YEAR_PATTERN = re.compile("^[0-9][0-9][0-9][0-9]$")
REGEX_NUM_YMD_PATTERN = re.compile("^\d{4}([-|/|.]\d{1,2})([-|/|.]\d{1,2})$")
REGEX_NUM_DMY_PATTERN = re.compile("^\d{1,2}([-|/|.]\d{1,2})([-|/|.]\d{4})$")
REGEX_NUM_MDY_PATTERN = re.compile("^\d{1,2}([-|/|.]\d{1,2})([-|/|.]\d{4})$")
REGEX_NUM_YM_PATTERN = re.compile("^\d{4}[-|/|.]\d{1,2}$")
REGEX_NUM_MY_PATTERN = re.compile("^\d{1,2}[-|/|.]\d{4}$")

REGEX_YEAR_PATTERN = re.compile("^[0-9][0-9][0-9][0-9]$")
REGEX_STRING_DATE_PATTERN_DMY = re.compile("^[0-9]+ [A-z]+ [0-9][0-9][0-9][0-9]$")
REGEX_STRING_DATE_PATTERN_YMD = re.compile("^[0-9][0-9][0-9][0-9], [A-z]* [0-9]+$")
REGEX_STRING_DATE_PATTERN_MDY = re.compile("^[A-z]* [0-9]+, [0-9][0-9][0-9][0-9]$")

# REGEX_YEAR_PATTERN_RELAX = re.compile("[0-9][0-9][0-9][0-9]")
# dmy dates: https://en.wikipedia.org/wiki/Template:Use_dmy_dates
REGEX_TEXT_DMY_PATTERN = re.compile("[0-9]+ [A-z]* [0-9][0-9][0-9][0-9]")
# mdy dates: https://en.wikipedia.org/wiki/Template:Use_mdy_dates
REGEX_TEXT_MDY_PATTERN = re.compile("[A-z]* [0-9]+, [0-9][0-9][0-9][0-9]")
REGEX_TEXT_YMD_PATTERN = re.compile("[0-9][0-9][0-9][0-9], [A-z]* [0-9]+")
REGEX_TEXT_MY_PATTERN = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December|january|february|march|april|may|june|july|august|september|october|november|december)\s\d{4}\b")

REGEX_TEXT_DATE_PATTERN_TIMESPAN1 = re.compile(
    r"\d{4},\s\w+\s\d{1,2}(?:\u2013\w+\s)?\d{1,2}")  # 2003, March 20\u2013May 22
REGEX_TEXT_DATE_PATTERN_TIMESPAN2 = re.compile(r"\d{4}\u2013\d{4}")  # 2003\u20132005
REGEX_TEXT_DATE_PATTERN_TIMESPAN3 = re.compile(r"\d{4},\s\w+\s\d{1,2}\u2013\d{1,2}")  # 2003, March 20\u201322
REGEX_TEXT_DATE_PATTERN_TIMESPAN4 = re.compile(r"\d{1,2}\s\w+\s\d{4}\s\u2013\s\d{4}")  # 24 May 2001 \u2013 2008
REGEX_TEXT_DATE_PATTERN_TIMESPAN5 = re.compile(
    r"\d{1,2}\s\w+\s\d{4}\s\u2013\s\d{1,2}\s\w+\s\d{4}")  # 29 May 2000 \u2013 13 July 2000
REGEX_TEXT_DATE_PATTERN_TIMESPAN6 = re.compile(
    r"\w+\s\d{1,2},\s\d{4}\s\u2013\s\w+\s\d{1,2},\s\d{4}")  # May 29, 2000 \u2013 July 13, 2000

TIMESTAMP_PATTERN_1 = re.compile('^"[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T00:00:00Z"')
TIMESTAMP_PATTERN_2 = re.compile("^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T00:00:00Z")
TIMESTAMP_PATTERN_3 = re.compile("^[-][0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T00:00:00Z")


def year_timespan(year):
    begin = f"{year}-01-01T00:00:00Z"
    end = f"{year}-12-31T00:00:00Z"
    return (begin, end)


def ym_timespan(ym):
    begin = f"{ym}-01T00:00:00Z"
    end = f"{ym}-31T00:00:00Z"
    return (begin, end)


def ymd_timespan(ymd):
    begin = f"{ymd}T00:00:00Z"
    end = f"{ymd}T00:00:00Z"
    return (begin, end)


class DateNormalization:
    # A normalized date annotated by SuTime or Regex Expression
    def __init__(self, d):
        # normalized time span
        self.timespan = d['timespan']
        # date mention text
        self.text = d['text']
        # date mention text span
        self.span = d['span']
        # date annotation method
        self.method = d['method']
        # date timestamp
        self.disambiguation = d['disambiguation']

    def json_dict(self):
        # Simple dictionary representation
        return {
            'text': self.text,
            'method': self.method,
            'timespan': self.timespan,
            'span': self.span,
            'disambiguation': self.disambiguation
        }


class SutimeDate:
    # A date annotated by SUTIME with reference date
    def __init__(self, d):
        self.span = (d['start'], d['end'])
        # date mention text
        self.text = d['text']
        # date type
        self.type = d['type']
        # value
        self.value = d['value']
        # reference date
        self.reference = d['reference']

    def json_dict(self):
        # Simple dictionary representation
        return {'text': self.text,
                'type': self.type,
                'span': self.span,
                'value': self.value,
                'reference': self.reference
                }


# class RegexDate:
#     # A date annotated by regular expression
#     def __init__(self, d):
#         self.span = (d['start'], d['end'])
#         # date mention text
#         self.text = d['text']
#         self.value = d['value']
#         self.timestamp = d['timestamp']
#
#     def json_dict(self):
#         # Simple dictionary representation
#         return {'text': self.text,
#                 'span': self.span,
#                 'value': self.value,
#                 'timestamp': self.timestamp
#                 }

"""Can be used for annotating dates in questions and evidences."""

# class SutimeAnnotator:
#     # ten years such as 194X: 1940-1949, 180X: 1800-1809
#     # one hundred years: 19XX
#     TIMEX_DECADE_PATTERN = re.compile("^[0-9][0-9][0-9]X$")
#     TIMEX_CENTURY_PATTERN = re.compile("^[0-9][0-9]XX$")
#     # YEAR-Season such as "2001-SP","2001-SU","2001-FA","2001-WI"
#     TIMEX_YEAR_SEASON_PATTERN = re.compile("^[0-9][0-9][0-9][0-9]-([SU|FA|WI|SP]{2}?)$")
#     # timex date in yymmdd format
#     TIMEX_YMD_PATTERN = re.compile("^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$")
#     TIMEX_YMD_PATTERN_BC = re.compile("^[-][0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$")
#     # timex date in yy format
#     TIMEX_YEAR_PATTERN = re.compile("^[0-9][0-9][0-9][0-9]$")
#     TIMEX_YEAR_PATTERN_BC = re.compile("^[-][0-9][0-9][0-9][0-9]$")
#     # timex date in yy-mm format
#     TIMEX_YM_PATTERN = re.compile("^[0-9][0-9][0-9][0-9]-[0-9][0-9]$")
#     # timex duration in P2014Y format
#     TIMEX_PYD_PATTERN = re.compile("^P[0-9][0-9][0-9][0-9]Y$")
#     # timex date in YYYY-MM-DDT00:00 format
#     # TIMEX_YMDT_PATTERN = re.compile("^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[\w\W]*$")
#     # # timex date in THIS P1Y INTERSECT YYYY ("the year of") format
#     TIMEX_INTERSECT_YEAR = re.compile("^THIS P1Y INTERSECT [0-9][0-9][0-9][0-9]$")
#     # # timex date in THIS P1Y INTERSECT YYYY-MM-DD ("the year of") format
#     TIMEX_INTERSECT_YMD = re.compile("^THIS P1Y INTERSECT [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$")
#     # exclusive text pattern
#     SIGNAL_IN_TEXT = re.compile("^[\w\W]*(before|after|prior to|in|start|begin|beginning|end)[\w\W]*$")
#     YEAR_IN_TEXT = re.compile(
#         "^(years|yearly|year|the years|the year|month of the year|the same year|the same day|the day|summer|winter|spring|autumn|fall)$")
#
#     def __init__(self):
#         self.sutime = SUTime(mark_time_ranges=True, include_range=True)
#
#     # sutime annotation basic method
#     def sutime_annotation(self, sentence, reference_date):
#         result = self.sutime.parse(sentence, reference_date)
#         date_annotations = []
#         try:
#             for tag in result:
#                 if "value" in tag:
#                     date_annotations.append(SutimeDate(
#                         {'value': tag['value'], 'text': tag['text'], 'start': tag['start'], 'end': tag['end'],
#                          'type': tag['type'], 'reference': reference_date}))
#             # date_annotations = [SutimeDate({'value': tag['value'], 'text': tag['text'], 'start': tag['start'], 'end': tag['end'], 'type': tag['type'], 'reference': reference_date}) for tag in
#             # result]
#             return [w.json_dict() for w in date_annotations]
#         except ValueError:
#             print("Error! That was no valid tag...", sentence)
#             print("Error! That was no valid tag...", result)
#
#             # sentences include sentence and ites reference time.
#
#     # for example, ['which violent events happened from 1998 to 1999', '2022']
#     def sutime_multithreading(self, sentences):
#         with ThreadPoolExecutor(max_workers=5) as executor:
#             annotation_sentences = [future.result()
#                                     for future in [executor.submit(self.sutime_annotation, sentence, reference_date)
#                                                    for sentence, reference_date in sentences
#                                                    ]]
#         return annotation_sentences
#
#     # convert the annotation result into standard format
#     def normalization(self, sentence_annotation_result):
#         date_norms = []
#         for annotation in sentence_annotation_result:
#             if not annotation: return []
#             if self.sutime_annotation_error(annotation['text']): continue
#             if annotation['type'] == 'DATE':
#                 result = self.timex_date_pattern(annotation['value'], annotation['reference'])
#                 if result:
#                     date_normalization = DateNormalization(
#                         {'text': annotation['text'], 'span': annotation['span'], 'timespan': result[1],
#                          'method': 'sutime', 'disambiguation': [(result[0], result[1][0])]})
#                     date_norms.append(date_normalization)
#             elif annotation['type'] == 'DURATION':
#                 if isinstance(annotation['value'], dict) and 'begin' in annotation['value'] and 'end' in annotation[
#                     'value']:
#                     result_begin = self.timex_date_pattern(annotation['value']['begin'], annotation['reference'])
#                     result_end = self.timex_date_pattern(annotation['value']['end'], annotation['reference'])
#                     if result_begin and result_end:
#                         date_normalization = DateNormalization(
#                             {'text': annotation['text'], 'span': annotation['span'],
#                              'timespan': (result_begin[1][0], result_end[1][1]),
#                              'method': 'sutime',
#                              'disambiguation': [(result_begin[0], result_begin[1][0]),
#                                                 (result_end[0], result_end[1][0])]})
#                         date_norms.append(date_normalization)
#         return [w.json_dict() for w in date_norms]
#
#     # check whether there is signal word such as "during" or "year" in the annotation result
#     def sutime_annotation_error(self, annotation_text):
#         if self.SIGNAL_IN_TEXT.match(annotation_text):
#             return True
#         if self.YEAR_IN_TEXT.match(annotation_text):
#             return True
#         return False
#
#     # convert sutime annotation result into standard format
#     def sutime_annotation_normalization(self, sentence, reference_date):
#         sentence_annotation_result = self.sutime_annotation(sentence, reference_date)
#         return self.normalization(sentence_annotation_result)
#
#     # multithread convert sutime annotation result into standard format
#     def sutime_annotation_normalization_multithreading(self, sentences):
#         annotation_sentences = self.sutime_multithreading(sentences)
#         """
#         normalize annotation with time span and the date is in standard formats YYYY-MM-DDT00:00:00Z
#         """
#         sentences_date_norms = []
#         for annotations in annotation_sentences:
#             sentences_date_norms.append(self.normalization(annotations))
#
#         return sentences_date_norms
#
#     def timex_date_pattern(self, date, reference_time):
#         # convert date value to standard format as timestamp in WikiData
#         """
#         Generate range for the timestamp.
#         Range of YYYY is [YYYY-01-01, YYYY-12-31]
#         Range of YYYY-MM [YYYY-MM-01, YYYY-MM-31]
#         Range of YYYY-MM-DD is [YYYY-MM-DD, YYYY-MM-DD]
#         """
#         PRESENT_REF_YEAR = reference_time.rsplit('-', 2)[0]
#         if self.TIMEX_YMD_PATTERN.match(date):
#             return date, ymd_timespan(date)
#
#         elif self.TIMEX_YMD_PATTERN_BC.match(date):
#             return date, ymd_timespan(date)
#
#         elif self.TIMEX_YEAR_PATTERN.match(date):
#             return date, year_timespan(date)
#
#         elif self.TIMEX_YEAR_PATTERN_BC.match(date):
#             return date, year_timespan(date)
#
#         elif self.TIMEX_YM_PATTERN.match(date):
#             return date, ym_timespan(date)
#
#         elif self.TIMEX_PYD_PATTERN.match(date):
#             year = date.replace('P', '').replace('Y', '')
#             return year, year_timespan(year)
#
#         # elif self.TIMEX_YMDT_PATTERN.match(date):
#         #     ymd = date.split('T')[0]
#         #     return ymd, ymd_timespan(ymd)
#
#         elif self.TIMEX_INTERSECT_YEAR.match(date):
#             year = date.replace('THIS P1Y INTERSECT ', '')
#             return year, year_timespan(year)
#
#         elif self.TIMEX_INTERSECT_YMD.match(date):
#             ymd = date.replace('THIS P1Y INTERSECT ', '')
#             return ymd, ymd_timespan(ymd)
#
#         elif 'PRESENT_REF' in date:
#             begin_timestamp = f"{PRESENT_REF_YEAR}-01-01T00:00:00Z"
#             end_timestamp = f"{PRESENT_REF_YEAR}-12-31T00:00:00Z"
#             return reference_time, (begin_timestamp, end_timestamp)
#
#         elif self.TIMEX_YEAR_SEASON_PATTERN.match(date):
#             year = f"{date.split('-')[0]}"
#             begin_timestamp = f"{year}-01-01T00:00:00Z"
#             end_timestamp = f"{year}-12-31T00:00:00Z"
#             return year, (begin_timestamp, end_timestamp)
#
#         elif self.TIMEX_DECADE_PATTERN.match(date):
#             decade_year = f"{date.rstrip('X')}0"
#             begin_timestamp = f"{date.rstrip('X')}0-01-01T00:00:00Z"
#             end_timestamp = f"{date.rstrip('X')}9-12-31T00:00:00Z"
#             return decade_year, (begin_timestamp, end_timestamp)
#
#         elif self.TIMEX_CENTURY_PATTERN.match(date):
#             century_year = f"{date.rstrip('XX')}00"
#             begin_timestamp = f"{date.rstrip('XX')}00-01-01T00:00:00Z"
#             end_timestamp = f"{date.rstrip('XX')}99-12-31T00:00:00Z"
#             return century_year, (begin_timestamp, end_timestamp)
#

"""Can be used for annotating dates and ordinals in questions and evidences."""


class RegexpAnnotator:
    @staticmethod
    def convert_number_to_month(number):
        """Map the given month to a number."""
        return {
            "01": "January",
            "02": "February",
            "03": "March",
            "04": "April",
            "05": "May",
            "06": "June",
            "07": "July",
            "08": "August",
            "09": "September",
            "10": "October",
            "11": "November",
            "12": "December",
        }[number]

    @staticmethod
    def is_date(string):
        """Return if the given string is a date."""
        return REGEX_STRING_DATE_PATTERN_DMY.match(string.strip()) != None

    @staticmethod
    def is_ymd_date(string):
        """
        Return if the given string is a mdy date.
        mdy format: https://en.wikipedia.org/wiki/Template:Use_mdy_dates
        """
        return REGEX_STRING_DATE_PATTERN_YMD.match(string.strip()) != None

    @staticmethod
    def is_mdy_date(string):
        """
        Return if the given string is a mdy date.
        mdy format: https://en.wikipedia.org/wiki/Template:Use_mdy_dates
        """
        return REGEX_STRING_DATE_PATTERN_MDY.match(string.strip()) != None

    @staticmethod
    def is_year(string):
        """Return if the given string describes a year in the format YYYY."""
        return REGEX_YEAR_PATTERN.match(string.strip()) != None

    @staticmethod
    def is_timestamp(string):
        """Return if the given string is a timestamp."""
        if TIMESTAMP_PATTERN_1.match(string.strip()) or TIMESTAMP_PATTERN_2.match(
                string.strip()) or TIMESTAMP_PATTERN_3.match(string.strip()):
            return True
        else:
            return False

    @staticmethod
    def get_year(timestamp):
        """Extract the year from the given timestamp."""
        return timestamp.split("-")[0].replace('"', '')

    @staticmethod
    def convert_year_to_timestamp(year):
        """Convert a year to a timestamp style."""
        return f"{year}-01-01T00:00:00Z"

    @staticmethod
    def convert_date_to_timestamp(date, date_format="dmy"):
        """Convert a date from the Wikidata frontendstyle to timestamp style."""
        if date_format == "dmy":
            return RegexpAnnotator._convert_dmy_to_timestamp(date)
        elif date_format == "ymd":
            return RegexpAnnotator._convert_ymd_to_timestamp(date)
        elif date_format == "timespan1":
            return RegexpAnnotator._convert_timespan1_to_timestamp(date)
        elif date_format == "timespan2":
            return RegexpAnnotator._convert_timespan2_to_timestamp(date)
        elif date_format == "timespan3":
            return RegexpAnnotator._convert_timespan3_to_timestamp(date)
        elif date_format == "timespan4":
            return RegexpAnnotator._convert_timespan4_to_timestamp(date)
        elif date_format == "timespan5":
            return RegexpAnnotator._convert_timespan5_to_timestamp(date)
        elif date_format == "timespan6":
            return RegexpAnnotator._convert_timespan6_to_timestamp(date)
        elif date_format == "my":
            return RegexpAnnotator._convert_my_to_timestamp(date)
        else:
            return RegexpAnnotator._convert_mdy_to_timestamp(date)

    @staticmethod
    def convert_month_to_number(month):
        """Map the given month to a number."""
        return {
            "january": "01",
            "jan": "01",
            "february": "02",
            "feb": "02",
            "march": "03",
            "mar": "03",
            "april": "04",
            "apr": "04",
            "may": "05",
            "june": "06",
            "jun": "06",
            "july": "07",
            "jul": "07",
            "august": "08",
            "aug": "08",
            "september": "09",
            "sep": "09",
            "october": "10",
            "oct": "10",
            "november": "11",
            "nov": "11",
            "december": "12",
            "dec": "12",
        }[month.lower()]

    @staticmethod
    def _convert_dmy_to_timestamp(date):
        """
        Convert a date in dmy format to timestamp style.
        dmy format: https://en.wikipedia.org/wiki/Template:Use_dmy_dates
        """
        try:
            adate = date.split(" ")
            # add the leading zero
            if len(adate[0]) < 2:
                adate[0] = f"0{adate[0]}"
            # create timestamp
            year = adate[2]
            month = RegexpAnnotator.convert_month_to_number(adate[1])
            day = adate[0]
            timestamp = f"{year}-{month}-{day}T00:00:00Z"
            return f"{year}-{month}-{day}", timestamp
        except:
            # print(f"Failure with dmy {date}")
            return None

    @staticmethod
    def _convert_timespan1_to_timestamp(date):
        """
                        Convert a date in ymd format to timestamp style.
                        ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                        """
        try:
            # –|-
            adate = []
            date = date.replace(", ", " ")
            if "–" in date:
                adate = date.split("–")
            elif "\u2013" in date:
                adate = date.split("\u2013")

            if len(adate) == 2:
                # remove comma and add the leading zero
                adate_part1 = adate[0].split(" ")
                adate_part1[2] = adate_part1[2].replace(",", "")
                if len(adate_part1[2]) < 2:
                    adate_part1[2] = f"0{adate_part1[2]}"
                # create timestamp
                year = adate_part1[0]
                month1 = RegexpAnnotator.convert_month_to_number(adate_part1[1])
                day1 = adate_part1[2]
                timestamp1_str = f"{year}-{month1}-{day1}"
                timestamp1 = f"{year}-{month1}-{day1}T00:00:00Z"

                # remove comma and add the leading zero
                adate_part2 = adate[1].split(" ")
                adate_part2[1] = adate_part2[1].replace(",", "")
                if len(adate_part2[1]) < 2:
                    adate_part2[1] = f"0{adate_part2[1]}"
                # create timestamp
                month2 = RegexpAnnotator.convert_month_to_number(adate_part2[0])
                day2 = adate_part2[1]
                timestamp2_str = f"{year}-{month2}-{day2}"
                timestamp2 = f"{year}-{month2}-{day2}T00:00:00Z"

                return timestamp1_str, timestamp1, timestamp2_str, timestamp2
        except:
            # print(f"Failure with timespan1 {date}")
            return None

    @staticmethod
    def _convert_timespan2_to_timestamp(date):
        """
                        Convert a date in ymd format to timestamp style.
                        ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                        """
        try:
            # –|-
            # –|-
            date = date.replace(", ", " ")
            adate = []
            if "–" in date:
                adate = date.split("–")

            elif "\u2013" in date:
                adate = date.split("\u2013")

            if len(adate) == 2:
                # remove comma and add the leading zero
                # create timestamp
                year1 = adate[0]
                timestamp1_str = f"{year1}-01-01"
                timestamp1 = f"{year1}-01-01T00:00:00Z"

                # remove comma and add the leading zero
                year2 = adate[1]
                timestamp2_str = f"{year2}-12-31"
                timestamp2 = f"{year2}-12-31T00:00:00Z"
                return timestamp1_str, timestamp1, timestamp2_str, timestamp2
        except:
            # print(f"Failure with timespan2 {date}")
            return None

    @staticmethod
    def _convert_timespan3_to_timestamp(date):
        """
                        Convert a date in ymd format to timestamp style.
                        ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                        """
        try:
            # –|-
            date = date.replace(", ", " ")
            adate = []
            if "–" in date:
                adate = date.split("–")
            elif "\u2013" in date:
                adate = date.split("\u2013")

            if len(adate) == 2:
                # remove comma and add the leading zero
                adate_part1 = adate[0].split(" ")
                year = adate_part1[0]
                month = RegexpAnnotator.convert_month_to_number(adate_part1[1])
                if len(adate_part1[2]) < 2:
                    adate_part1[2] = f"0{adate_part1[2]}"
                # create timestamp

                day1 = adate_part1[2]
                timestamp1_str = f"{year}-{month}-{day1}"
                timestamp1 = f"{year}-{month}-{day1}T00:00:00Z"

                # remove comma and add the leading zero
                adate_part2 = adate[1]
                if len(adate_part2) < 2:
                    adate_part2 = f"0{adate_part2}"
                # create timestamp
                day2 = adate_part2
                timestamp2_str = f"{year}-{month}-{day2}"
                timestamp2 = f"{year}-{month}-{day2}T00:00:00Z"

                return timestamp1_str, timestamp1, timestamp2_str, timestamp2
        except:
            # print(f"Failure with timespan3 {date}")
            return None

    # REGEX_TEXT_DATE_PATTERN_TIMESPAN4 = r"\d{1,2}\s\w+\s\(d{4})\s\u2013\s(\d{4})"  # 24 May 2001 \u2013 2008
    @staticmethod
    def _convert_timespan4_to_timestamp(date):
        """
                        Convert a date in ymd format to timestamp style.
                        ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                        """
        try:
            # –|-
            adate = []
            if "–" in date:
                adate = date.split("–")
            elif "\u2013" in date:
                adate = date.split("\u2013")

            if len(adate) == 2:
                # remove comma and add the leading zero
                adate_part1 = adate[0].strip().split(" ")
                year = adate_part1[2]
                month = RegexpAnnotator.convert_month_to_number(adate_part1[1])
                if len(adate_part1[0]) < 2:
                    adate_part1[0] = f"0{adate_part1[0]}"
                # create timestamp

                day1 = adate_part1[0]
                timestamp1_str = f"{year}-{month}-{day1}"
                timestamp1 = f"{year}-{month}-{day1}T00:00:00Z"

                # remove comma and add the leading zero
                adate_part2 = adate[1].strip()
                # create timestamp
                year = adate_part2
                timestamp2_str = f"{year}-01-01"
                timestamp2 = f"{year}-12-31T00:00:00Z"

                return timestamp1_str, timestamp1, timestamp2_str, timestamp2
        except:
            # print(f"Failure with timespan4 {date}")
            return None

    # REGEX_TEXT_DATE_PATTERN_TIMESPAN5 = r"\d{1,2}\s\w+\s\(d{4})\s\u2013\s(\d{1,2}\s\w+\s\(d{4})"  # 29 May 2000 \u2013 13 July 2000
    @staticmethod
    def _convert_timespan5_to_timestamp(date):
        """
                        Convert a date in ymd format to timestamp style.
                        ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                        """
        try:
            # –|-
            adate = []
            if "–" in date:
                adate = date.split("–")
            elif "\u2013" in date:
                adate = date.split("\u2013")

            if len(adate) == 2:
                # remove comma and add the leading zero
                adate_part1 = adate[0].strip().split(" ")
                year1 = adate_part1[2]
                month1 = RegexpAnnotator.convert_month_to_number(adate_part1[1])
                if len(adate_part1[0]) < 2:
                    adate_part1[0] = f"0{adate_part1[0]}"
                # create timestamp

                day1 = adate_part1[0]
                timestamp1_str = f"{year1}-{month1}-{day1}"
                timestamp1 = f"{year1}-{month1}-{day1}T00:00:00Z"

                # remove comma and add the leading zero
                adate_part2 = adate[1].strip().split(" ")
                # create timestamp
                year2 = adate_part2[2]
                month2 = RegexpAnnotator.convert_month_to_number(adate_part2[1])
                if len(adate_part2[0]) < 2:
                    adate_part2[0] = f"0{adate_part2[0]}"
                # create timestamp

                day2 = adate_part2[0]
                timestamp2_str = f"{year2}-{month2}-{day2}"
                timestamp2 = f"{year2}-{month2}-{day2}T00:00:00Z"

                return timestamp1_str, timestamp1, timestamp2_str, timestamp2
        except:
            # print(f"Failure with timespan5 {date}")
            return None

    @staticmethod
    ##re.compile(r"\w+\s\d{1,2},\s\d{4}\s\u2013\s\w+\s\d{1,2},\s\d{4}")  # May 29, 2000 \u2013 July 13, 2000
    def _convert_timespan6_to_timestamp(date):
        """
                        Convert a date in ymd format to timestamp style.
                        ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                        """
        try:
            # –|-
            date = date.replace(", ", " ")
            adate = []
            if "–" in date:
                adate = date.split("–")
            elif "\u2013" in date:
                adate = date.split("\u2013")

            if len(adate) == 2:
                # remove comma and add the leading zero
                adate_part1 = adate[0].strip().split(" ")
                year1 = adate_part1[2]
                month1 = RegexpAnnotator.convert_month_to_number(adate_part1[0])
                if len(adate_part1[1]) < 2:
                    adate_part1[1] = f"0{adate_part1[1]}"
                # create timestamp

                day1 = adate_part1[1]
                timestamp1_str = f"{year1}-{month1}-{day1}"
                timestamp1 = f"{year1}-{month1}-{day1}T00:00:00Z"

                # remove comma and add the leading zero
                adate_part2 = adate[1].strip().split(" ")
                # create timestamp
                year2 = adate_part2[2]
                month2 = RegexpAnnotator.convert_month_to_number(adate_part2[0])
                if len(adate_part2[1]) < 2:
                    adate_part2[1] = f"0{adate_part2[1]}"
                # create timestamp

                day2 = adate_part2[1]
                timestamp2_str = f"{year2}-{month2}-{day2}"
                timestamp2 = f"{year2}-{month2}-{day2}T00:00:00Z"

                return timestamp1_str, timestamp1, timestamp2_str, timestamp2
        except:
            # print(f"Failure with timespan6 {date}")
            return None

    @staticmethod
    ##REGEX_TEXT_DATE_PATTERN_TIMESPAN7 = re.compile(r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}\b")
    def _convert_my_to_timestamp(date):
        """
                        Convert a date in ymd format to timestamp style.
                        ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                        """
        try:
            # –|-
            adate = date.strip().split(" ")
            if len(adate) == 2:
                # remove comma and add the leading zero
                year = adate[1].strip()
                month = RegexpAnnotator.convert_month_to_number(adate[0].strip())
                # create timestamp
                timestamp1_str = f"{year}-{month}-01"
                timestamp1 = f"{year}-{month}-01T00:00:00Z"
                timestamp2_str = f"{year}-{month}-31"
                timestamp2 = f"{year}-{month}-31T00:00:00Z"

                return timestamp1_str, timestamp1, timestamp2_str, timestamp2
        except:
            # print(f"Failure with my {date}")
            return None

    @staticmethod
    def _convert_ymd_to_timestamp(date):
        """
                Convert a date in ymd format to timestamp style.
                ymd format: https://en.wikipedia.org/wiki/Template:Use_ymd_dates
                """
        try:
            date = date.replace(", ", " ")
            adate = date.split(" ")
            # remove comma and add the leading zero
            if len(adate[2]) < 2:
                adate[2] = f"0{adate[2]}"
            # create timestamp
            year = adate[0]
            month = RegexpAnnotator.convert_month_to_number(adate[1])
            day = adate[2]
            timestamp = f"{year}-{month}-{day}T00:00:00Z"
            return f"{year}-{month}-{day}", timestamp
        except:
            # print(f"Failure with mdy {date}")
            return None

    @staticmethod
    def _convert_mdy_to_timestamp(date):
        """
        Convert a date in mdy format to timestamp style.
        mdy format: https://en.wikipedia.org/wiki/Template:Use_mdy_dates
        """
        try:
            adate = date.split(" ")
            # remove comma and add the leading zero
            adate[1] = adate[1].replace(",", "")
            if len(adate[1]) < 2:
                adate[1] = f"0{adate[1]}"
            # create timestamp
            year = adate[2]
            month = RegexpAnnotator.convert_month_to_number(adate[0])
            day = adate[1]
            timestamp = f"{year}-{month}-{day}T00:00:00Z"
            return f"{year}-{month}-{day}", timestamp
        except:
            # print(f"Failure with mdy {date}")
            return None

    @staticmethod
    def convert_timestamp_to_normalized_date(timestamp):
        date = timestamp.replace("T00:00:00Z", "")
        return date

    @staticmethod
    def convert_timestamp_to_date(timestamp):
        """Convert the given timestamp to the corresponding date."""
        try:
            adate = timestamp.rsplit("-", 2)
            # parse data
            year = adate[0]
            month = RegexpAnnotator.convert_number_to_month(adate[1])
            day = adate[2].split("T")[0]
            # remove leading zero
            if day[0] == "0":
                day = day[1]
            if day == "1" and adate[1] == "01":
                # return year for 1st jan
                return year
            date = f"{day} {month} {year}"
            return date
        except:
            # print(f"Failure with timestamp {timestamp}")
            return timestamp

    def remove_punctuation_in_token(self, token):
        punctuations = ['.', ';', '(', ')', '[', ']', ',']
        for punc in punctuations:
            token = token.rstrip(punc)
            token = token.lstrip(punc)
        return token

    def normalize_ymd_date_pattern(self, year, mm, dd):
        try:
            if len(mm) == 1:
                mm = "0" + mm
            if len(dd) == 1:
                dd = "0" + dd
            if int(mm) == 0:
                date = year
                timespan = year_timespan(year)
                return date, timespan
            elif int(mm) >= 1 and int(mm) <= 12 and int(dd) == 0:
                date = year + '-' + mm
                timespan = ym_timespan(date)
                return date, timespan
            elif int(mm) >= 1 and int(mm) <= 12 and int(dd) >= 1 and int(dd) <= 31:
                date = year + '-' + mm + '-' + dd
                timespan = ymd_timespan(date)
                return date, timespan
        except:
            # print(f"Failure with normalize {year} {mm} {dd}")
            return None

    def extract_dates_in_text_format(self, string):
        date_norms = []
        # dates in dmy format
        dmy_dates = re.findall(REGEX_TEXT_DMY_PATTERN, string)

        for match in dmy_dates:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="dmy")
            if result:
                date = result[0]
                timestamp = result[1]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp, timestamp),
                     'method': 'regex',
                     'disambiguation': [(date, timestamp)]})
                date_norms.append(date_normalization)

        # two dates in timespan1 format
        timespan1 = re.findall(REGEX_TEXT_DATE_PATTERN_TIMESPAN1, string)
        for match in timespan1:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="timespan1")
            # timestamp1_str, timestamp1, timestamp2_str, timestamp2
            if result:
                date1 = result[0]
                timestamp1 = result[1]
                date2 = result[2]
                timestamp2 = result[3]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp1, timestamp2),
                     'method': 'regex',
                     'disambiguation': [(date1, timestamp1), (date2, timestamp2)]})
                date_norms.append(date_normalization)

        # two dates in timespan2 format
        timespan2 = re.findall(REGEX_TEXT_DATE_PATTERN_TIMESPAN2, string)
        for match in timespan2:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="timespan2")
            if result:
                date1 = result[0]
                timestamp1 = result[1]
                date2 = result[2]
                timestamp2 = result[3]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp1, timestamp2),
                     'method': 'regex',
                     'disambiguation': [(date1, timestamp1), (date2, timestamp2)]})
                date_norms.append(date_normalization)

        # two dates in timespan3 format
        timespan3 = re.findall(REGEX_TEXT_DATE_PATTERN_TIMESPAN3, string)
        for match in timespan3:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="timespan3")
            if result:
                date1 = result[0]
                timestamp1 = result[1]
                date2 = result[2]
                timestamp2 = result[3]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp1, timestamp2),
                     'method': 'regex',
                     'disambiguation': [(date1, timestamp1), (date2, timestamp2)]})
                date_norms.append(date_normalization)

        # two dates in timespan4 format
        timespan4 = re.findall(REGEX_TEXT_DATE_PATTERN_TIMESPAN4, string)
        for match in timespan4:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="timespan4")
            if result:
                date1 = result[0]
                timestamp1 = result[1]
                date2 = result[2]
                timestamp2 = result[3]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp1, timestamp2),
                     'method': 'regex',
                     'disambiguation': [(date1, timestamp1), (date2, timestamp2)]})
                date_norms.append(date_normalization)

        # two dates in timespan5 format
        timespan5 = re.findall(REGEX_TEXT_DATE_PATTERN_TIMESPAN5, string)
        for match in timespan5:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="timespan5")
            if result:
                date1 = result[0]
                timestamp1 = result[1]
                date2 = result[2]
                timestamp2 = result[3]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp1, timestamp2),
                     'method': 'regex',
                     'disambiguation': [(date1, timestamp1), (date2, timestamp2)]})
                date_norms.append(date_normalization)

        # two dates in timespan6 format
        timespan6 = re.findall(REGEX_TEXT_DATE_PATTERN_TIMESPAN6, string)
        for match in timespan6:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="timespan6")
            if result:
                date1 = result[0]
                timestamp1 = result[1]
                date2 = result[2]
                timestamp2 = result[3]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp1, timestamp2),
                     'method': 'regex',
                     'disambiguation': [(date1, timestamp1), (date2, timestamp2)]})
                date_norms.append(date_normalization)

        # month year format
        monthyear = re.findall(REGEX_TEXT_MY_PATTERN, string)
        for match in monthyear:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="my")
            if result:
                date1 = result[0]
                timestamp1 = result[1]
                date2 = result[2]
                timestamp2 = result[3]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp1, timestamp2),
                     'method': 'regex',
                     'disambiguation': [(date1, timestamp1)]})
                date_norms.append(date_normalization)

        # dates in ymd format
        ymd_dates = re.findall(REGEX_TEXT_YMD_PATTERN, string)
        for match in ymd_dates:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="ymd")
            if result:
                date = result[0]
                timestamp = result[1]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp, timestamp),
                     'method': 'regex',
                     'disambiguation': [(date, timestamp)]})
                date_norms.append(date_normalization)

        # dates in mdy format
        mdy_dates = re.findall(REGEX_TEXT_MDY_PATTERN, string)
        for match in mdy_dates:
            patt_start = string.index(match)
            patt_end = patt_start + len(match)
            result = RegexpAnnotator.convert_date_to_timestamp(match, date_format="mdy")
            if result:
                date = result[0]
                timestamp = result[1]
                date_normalization = DateNormalization(
                    {'text': match, 'span': (patt_start, patt_end), 'timespan': (timestamp, timestamp),
                     'method': 'regex',
                     'disambiguation': [(date, timestamp)]})
                date_norms.append(date_normalization)

        return [w.json_dict() for w in date_norms]

    def extract_date_in_num_format(self, string):
        date_norms = []
        tokens = string.split(" ")
        for token in tokens:
            token_withno_punc = self.remove_punctuation_in_token(token)
            token_start = string.index(token_withno_punc)
            token_end = token_start + len(token_withno_punc)

            if REGEX_NUM_YEAR_PATTERN.match(token_withno_punc):
                timestamp = RegexpAnnotator.convert_year_to_timestamp(token_withno_punc)
                date_normalization = DateNormalization(
                    {'text': token_withno_punc, 'span': (token_start, token_end),
                     'timespan': (timestamp, f"{token_withno_punc}-12-31T00:00:00Z"),
                     'method': 'regex',
                     'disambiguation': [(token_withno_punc, timestamp)]})
                date_norms.append(date_normalization)

            if REGEX_NUM_YMD_PATTERN.match(token_withno_punc):
                year = re.split(r'[-|.|/]', token_withno_punc)[0]
                mm = re.split(r'[-|.|/]', token_withno_punc)[1]
                dd = re.split(r'[-|.|/]', token_withno_punc)[2]
                result = self.normalize_ymd_date_pattern(year, mm, dd)
                if result:
                    date = result[0]
                    timespan = result[1]
                    timestamp = timespan[0]
                    date_normalization = DateNormalization(
                        {'text': token_withno_punc, 'span': (token_start, token_end), 'timespan': timespan,
                         'method': 'regex',
                         'disambiguation': [(date, timestamp)]})
                    date_norms.append(date_normalization)

            elif REGEX_NUM_MDY_PATTERN.match(token_withno_punc):
                year = re.split(r'[-|.|/]', token_withno_punc)[2]
                mm = re.split(r'[-|.|/]', token_withno_punc)[0]
                dd = re.split(r'[-|.|/]', token_withno_punc)[1]
                result = self.normalize_ymd_date_pattern(year, mm, dd)
                if result:
                    date = result[0]
                    timespan = result[1]
                    timestamp = timespan[0]
                    date_normalization = DateNormalization(
                        {'text': token_withno_punc, 'span': (token_start, token_end), 'timespan': timespan,
                         'method': 'regex',
                         'disambiguation': [(date, timestamp)]})
                    date_norms.append(date_normalization)

                else:
                    year = re.split(r'[-|.|/]', token)[2]
                    mm = re.split(r'[-|.|/]', token)[1]
                    dd = re.split(r'[-|.|/]', token)[0]
                    result = self.normalize_ymd_date_pattern(year, mm, dd)
                    if result:
                        date = result[0]
                        timespan = result[1]
                        timestamp = timespan[0]
                        date_normalization = DateNormalization(
                            {'text': token_withno_punc, 'span': (token_start, token_end), 'timespan': timespan,
                             'method': 'regex',
                             'disambiguation': [(date, timestamp)]})
                        date_norms.append(date_normalization)

        return [w.json_dict() for w in date_norms]

    # multithread annotate sentences using regular expression and normalize them into standard format
    def regex_annotation_normalization_multithreading(self, string_refers):
        with ThreadPoolExecutor(max_workers=5) as executor:
            annotation_sentences = [future.result()
                                    for future in [executor.submit(self.regex_annotation_normalization, string)
                                                   for string, reference_time in string_refers
                                                   ]]
        return annotation_sentences

    # annotate sentences using regular expression and normalize them into standard format
    def regex_annotation_normalization(self, string):
        """
        Extract dates in text (added to entities).
        First, text is searched for text, then the dates
        are brought into a compatible format (timestamps).
        TODO: Will be replaced by global function in temporal library.
        """

        date_norms_text = self.extract_dates_in_text_format(string)
        date_norms_num = self.extract_date_in_num_format(string)
        date_norms = self.remove_duplicate_matched(date_norms_text, date_norms_num)
        return date_norms

    # remove duplicate annotation results
    def remove_duplicate_matched(self, annotations_text, annotations_number):
        disambiguations = {}
        for item in annotations_text:
            span = item['span']
            disambiguations[span] = item
        for item in annotations_number:
            span = item['span']
            disambiguations[span] = item

        start_end = list(disambiguations.keys())
        if len(start_end) > 1:
            for i in range(0, len(start_end) - 1):
                for j in range(i + 1, len(start_end)):
                    if not self.check_overlap(start_end[i], start_end[j]):
                        continue
                    else:
                        lengthi = start_end[i][1] - start_end[i][0]
                        lengthj = start_end[j][1] - start_end[j][0]
                        if lengthj >= lengthi:
                            if start_end[i] in disambiguations:
                                disambiguations.pop(start_end[i])
                        else:
                            if start_end[j] in disambiguations:
                                disambiguations.pop(start_end[j])

        return list(disambiguations.values())

    # check whether two results are overlap
    def check_overlap(self, rangei, rangej):
        start1 = rangei[0]
        end1 = rangei[1]
        start2 = rangej[0]
        end2 = rangej[1]

        if end1 < start2 or start1 > end2:
            return False
        elif start1 == start2 or end1 == end2 or start1 == end2 or start2 == end1:
            return True
        elif start1 < start2 and end1 > start2:
            return True
        elif start1 > start2 and start1 < end2:
            return True
