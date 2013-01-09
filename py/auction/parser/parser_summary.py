import tables
from tables import *
from datetime import datetime
from auction.time_utils import *
import sys

class ParseResults(IsDescription):
    """
    Record containing useful information on the result of parsing and
    generating the data
    """
    is_valid        = IntCol()
    src_file        = StringCol(128)
    src_file_size   = Int64Col()
    src_file_mtime  = Int64Col()
    parse_start     = Int64Col()
    parse_stop      = Int64Col()
    data_start      = Int64Col()
    data_stop       = Int64Col()
    processed       = Int64Col()
    # Track count of records that do not impact books (i.e. updates in the wings)
    irrelevants     = Int64Col()    

class ParseWarnings(IsDescription):
    """
    Record for logging warnings
    """
    src_line        = Int64Col()
    msg             = StringCol(256)

class ParseManager(object):
    """
    Class for tracking the progress of a parse generating script. Scripts
    parse market data text files and build books. This utility class is a
    place to add some progress metadata to the generated book data file
    """

    @staticmethod
    def summarize_file(h5_file_path):
        h5_file = tables.openFile(h5_file_path)
        summary_record = h5_file.root.parse_results.summary[0]
        print "Summary of:", h5_file_path
        print "\tis_valid:", summary_record['is_valid']
        print "\tsrc_file:", summary_record['src_file']
        print "\tsrc_file_size:", summary_record['src_file_size']
        print "\tsrc_file_mtime:", chicago_time(summary_record['src_file_mtime'])
        print "\tparse_start:", chicago_time(summary_record['parse_start'])
        print "\tparse_stop:", chicago_time(summary_record['parse_stop'])
        print "\tdata_start:", chicago_time(summary_record['data_start'])
        print "\tdata_stop:", chicago_time(summary_record['data_stop'])
        print "\tprocessed:", summary_record['processed']
        print "\tirrelevants:", summary_record['irrelevants']
        print "\ttotal warnings:", h5_file.root.parse_results.warnings.nrows


    def __init__(self, src_file, out_h5_file):
        self.__parse_start = 0
        self.__parse_stop = 0
        self.__data_start = 0
        self.__data_stop = 0
        self.__out_h5_file = out_h5_file
        group = self.__out_h5_file.createGroup("/", "parse_results", "Info about the parse")
        self.__summary = self.__out_h5_file.createTable(group, 
                                                        'summary', 
                                                        ParseResults, 
                                                        "Summary of parse results")
        self.__warnings = self.__out_h5_file.createTable(group, 
                                                         'warnings', 
                                                         ParseWarnings, 
                                                         "Any warnings during parsing")
        self.__summary_row = self.__summary.row
        self.__warning_row = self.__warnings.row        

        self.__summary_row['src_file'] = src_file.basename()
        self.__summary_row['src_file_size'] = src_file.size
        self.__summary_row['src_file_mtime'] = timestamp_from_mtime(src_file.mtime)


    def warning(self, msg, line=0):
        """
        Log a warning message with an optional src file line number
        """
        self.__warning_row['msg'] = msg
        self.__warning_row['src_line'] = line
        self.__warning_row.append()

    def data_start(self, start):
        """
        Track the start time of the market data
        """
        self.__summary_row['data_start'] = start

    def data_stop(self, stop):
        """
        Track the stop time of the market data
        """
        self.__summary_row['data_stop'] = stop

    def processed(self, count):
        """
        Number of records processed
        """
        self.__summary_row['processed'] = count

    def irrelevants(self, count):
        """
        Store the count of market updates that do not affect the book given
        the levels of data being stored. The main purpose is to record the
        number of updates that have no impact on the book.
        """
        self.__summary_row['irrelevants'] = count

    def mark_start(self):
        """
        Track the start time of the parse/generation
        """
        self.__parse_start = timestamp()

    def mark_stop(self, success):
        """
        Track the stop time of the parse/generation.
        This also writes out the summary info and flushes the file.
        """
        self.__parse_stop = timestamp()
        summary = self.__summary.row
        summary['parse_start'] = self.__parse_start
        summary['parse_stop'] = self.__parse_stop
        summary['is_valid'] = success and 1 or 0
        summary.append()
        self.__out_h5_file.flush()

if __name__ == "__main__":
    from auction.paths import *
    from path import path
    target_file = path('/tmp/summary_file_sample.h5')
    h5_file = openFile(target_file, mode = "w", title = "Parse summary record")
    pm = ParseManager(COMPRESSED_DATA_PATH / 'arca' / 'arcabookftp20070611.csv.gz', 
                      h5_file)
    pm.mark_start()
    pm.data_start(timestamp())
    pm.data_stop(timestamp())
    pm.warning("Things just went haywire", 72)
    pm.warning("Things went totally berzerk", 2001)
    pm.processed(5832)
    pm.irrelevants(29)
    pm.mark_stop(True)
    h5_file.close()
    ParseManager.summarize_file(target_file)
