###############################################################################
#
# File: cme_fix_parser.py
#
# Description: Parser for cme fix files
#
##############################################################################
from path import path
from attribute import readable, writable, attribute
from auction.paths import *
from auction.book import Book, BookTable
from auction.parser.parser_summary import ParseManager
from auction.parser.utils import PriceOrderedDict, FileRecordCounter, BookBuilder
from auction.time_utils import *
from tables import *
from copy import copy

import os
import zipfile
import gzip
import re
import logging
import sets
import string
import pprint
import traceback
import datetime

__CME_SRC_PATH__ = DATA_PATH / 'CME_GLOBEX2'
__CmeDateTimeRe__ = re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})")
__SecondCentisecondRe__ = re.compile(r"(\d{2})(\d{2})")
__BLANK_LINE__ = re.compile(r'^\s*$')
__LEVELS__ = 10


class RlcRecord(object):
    readable(levels=None, symbol=None, timestamp=None, trade_details=None)

    @staticmethod
    def make_timestamp(date_time, seconds_centis):
        m = __CmeDateTimeRe__.match(date_time)
        assert m, "Invalid timestamp:" + date_time
        year, mon, day, hr, minutes, sec = [ int(i) for i in m.groups() ]
        result = datetime.datetime(int(year), int(mon), int(day),
                                   int(hr),int(minutes),int(sec), 0,
                                   tzinfo=CHI_TZ).astimezone(UTC_TZ)
        m = __SecondCentisecondRe__.match(seconds_centis)
        if m:
            seconds, centisec = [ int(i) for i in m.groups() ]
            result = result + datetime.timedelta(0, (seconds-sec)%60, centisec*10000)
        return timestamp_from_datetime(result)

    def is_book_message(self):
        return self.__msg_type == 'MA'

    def is_trade_message(self):
        return self.__msg_type == 'M6'

    def __init__(self, line):
        self.__msg_type = line[33:35]
        if self.is_trade_message():
            self.__host_ts = line[12:17]
            self.__trade_date = line[41:49]
            self.__symbol = line[49:69].strip()
            self.__date_time = line[17:31]
            self.__timestamp = RlcRecord.make_timestamp(self.__date_time, self.__host_ts)
            # (px, qty, type)
            self.__trade_details = (int(line[81:100]), int(line[69:81]), int(line[188:189]))

        elif self.is_book_message():
            self.__host_ts = line[12:17]
            self.__trade_date = line[41:49]
            self.__symbol = line[49:69].strip()
            self.__date_time = line[17:31]
            self.__timestamp = RlcRecord.make_timestamp(self.__date_time, self.__host_ts)
            self.__trading_mode = line[70:71]
            self.__change_mask = line[76:81]
            self.__levels = []
            start_index = 0
            group_size = 72

            def r(a,b):
                return slice(start_index+a, start_index+b)

            for i, is_set in enumerate(self.__change_mask):
                if is_set=='1':
                    self.__levels.append({ 'level':i, 
                                           'symbol':self.__symbol,
                                           'total_buy':int(line[r(82,94)]),
                                           'num_buys': int(line[r(94,98)]),
                                           'buy_px':int(line[r(98,117)]),

                                           'sell_px':int(line[r(117,136)]),
                                           'num_sells':int(line[r(136,140)]),
                                           'total_sell':int(line[r(140,152)]), })
                    #print "Added ", pprint.pformat(self.__levels[-1])
                    start_index = start_index + group_size

            if abs(len(line) - (start_index+81)) > 1:
                #print len(line), (start_index+81)
                #print line
                #raise "Unexpected extra data on line: "+line
                pass
        else:
            return

class CmeRlcBookBuilder(BookBuilder):

    readable(bid_book=None, ask_book=None)

    def __init__(self, symbol, h5_file, prior_day_books, **rest):
        print "Creating builder ", symbol, h5_file
        BookBuilder.__init__(self, symbol, h5_file, **rest)
        if prior_day_books:
            self.__bid_book = prior_day_books[0]
            self.__ask_book = prior_day_books[1]
        else:
            self.__bid_book = [None]*__LEVELS__
            self.__ask_book = [None]*__LEVELS__

    def top_bid(self):
        tb = self.__bid_book[0]
        return tb[0] if tb else tb

    def top_ask(self):
        ta = self.__ask_book[0]
        return ta[0] if ta else ta

    def process_record(self, record):
        for update in record.levels:
            level = update['level']
            self.__bid_book[level] = (update['buy_px'], update['total_buy'])
            self.__ask_book[level] = (update['sell_px'], update['total_sell'])
        self.write_record(record.timestamp, chicago_time_str(record.timestamp))

    def write_record(self, ts, ts_s):
        # copy from book to record
        for i, pair in enumerate(self.__bid_book):
            if pair == None:
                self._bids[i][0] = 0
                self._bids[i][1] = 0
            else:
                self._bids[i][0] = pair[0]
                self._bids[i][1] = pair[1]

        for i, pair in enumerate(self.__ask_book):
            if pair == None:
                self._asks[i][0] = 0
                self._asks[i][1] = 0
            else:
                self._asks[i][0] = pair[0]
                self._asks[i][1] = pair[1]

        self._record['bid'] = self._bids
        self._record['ask'] = self._asks
        self._record['timestamp'] = ts
        self._record['timestamp_s'] = ts_s
        self._record.append()
        self._file_record_counter.increment_count()

    def write_trade(self, record):
        if self._trade:
            self._trade['timestamp'] = record.timestamp
            self._trade['timestamp_s'] = chicago_time_str(record.timestamp)
            trade_details = record.trade_details
            self._trade['price'] = trade_details[0]
            self._trade['quantity'] = trade_details[1]
            self._trade['trade_type'] = trade_details[2]
            self._trade.append()
        

class CmeRlcParser(object):
    r"""

"""

    readable(input_paths=None) 

    match_all = re.compile(".*")

    def __init__(self, input_path_list):
        """
        """
        self.__input_path_list = copy(input_path_list)
        self.__book_builders = {}
        self.__h5_file = None
        self.__ts = None
        self.__chi_ts = None
        self.__data_start_timestamp = None
        self.__current_timestamp = None
        self.__output_path = None
        self.__prior_day_books = {}

    def write_summary(self):
        ############################################################
        # Finish filling in the parse summary info and close up
        ############################################################
        self.__parse_manager.data_start(self.__data_start_timestamp)
        self.__parse_manager.data_stop(self.__current_timestamp)
        self.__parse_manager.irrelevants(0)
        self.__parse_manager.processed(self.__line_number+1)
        self.__parse_manager.mark_stop(True)
        self.__h5_file.close()
        ParseManager.summarize_file(self.__output_path)

    def advance_date(self, new_date):
        if self.__h5_file:
            self.write_summary()

        self.__output_path = CME_OUT_PATH / get_date_string(new_date)
        if not self.__output_path.parent.exists():
            os.makedirs(self.__output_path.parent)
        print "OUT", self.__output_path
        self.__h5_file = openFile(self.__output_path, mode="w", title="CME Fix Data")
        self.__parse_manager = ParseManager(self.__current_input_path, self.__h5_file)
        self.__parse_manager.mark_start()
        self.__prior_day_books = {}
        self.__data_start_timestamp = 0
        for symbol, builder in self.__book_builders.items():
            self.__prior_day_books[symbol] = (builder.bid_book, builder.ask_book)
        self.__book_builders = {}


    def build_books(self, record):
        try:
            symbol = record.symbol
            builder = self.__book_builders.get(symbol)
            if not builder:
                builder = CmeRlcBookBuilder(symbol, self.__h5_file, 
                                            self.__prior_day_books.get(symbol, None),
                                            include_trades = True)
                self.__book_builders[symbol] = builder


            if record.is_book_message():
                builder.process_record(record)
            elif record.is_trade_message():
                builder.write_trade(record)

        except Exception,e:
            print traceback.format_exc()
            self.__parse_manager.warning(self.__current_file + ':' + e.message, 
                                         'G', self.__current_timestamp,
                                         self.__line_number+1)

    def parse(self):
        i = 0
        for p in self.__input_path_list:
            self.__current_input_path = p
            date = get_date_of_file(p)
            self.advance_date(date)
            root = zipfile.ZipFile(self.__current_input_path, 'r')
            files = root.namelist()
            for f in files:
                print "Processing file", f, "count", i
                self.__line_number = 0
                self.__current_file = f
                for line in root.read(f).split("\n"):
                    i =i+1
                    if not  __BLANK_LINE__.match(line):
                        record = RlcRecord(line)
                        self.__current_timestamp = record.timestamp
                        if 0 == self.__data_start_timestamp:
                            self.__data_start_timestamp = record.timestamp
                        if record.is_book_message() or record.is_trade_message():
                            self.build_books(record)


                    self.__line_number += 1

            print "Completed", i , "records"
        self.write_summary()

if __name__ == "__main__":
    import pprint
    import argparse
    from auction.parser.cme_date_utils import CmeRawFileSet

    parser = argparse.ArgumentParser("""
Take input raw data and generate corresponding hdf5 data files as well as book
files for the raw data.
""")

    parser.add_argument('-d', '--date', 
                        dest='dates',
                        nargs='+',
                        action='store',
                        help='Dates to process')

    parser.add_argument('-v', '--verbose', 
                        dest='verbose',
                        action='store_true',
                        help='Output extra logging information')

    options = parser.parse_args()

    if options.verbose:
        logging.basicConfig(level=logging.INFO)

    fileset = CmeRawFileSet()
    files = fileset.get_files(options.dates)
    if len(files) != len(options.dates):
        print "Mismatch on files:", options.date, "\nvs\n\t", files
        exit(-1)
    parser = CmeRlcParser(files)
    parser.parse()
    pprint.pprint(vars(parser))

