###############################################################################
#
# File: cme_fix_parser.py
#
# Description: Parser for cme fix files
#
##############################################################################
from path import path
from attribute import readable, writable
from auction.paths import *
from auction.book import Book, BookTable
from auction.parser.parser_summary import ParseManager
from auction.parser.utils import PriceOrderedDict, FileRecordCounter, BookBuilder
from auction.time_utils import *
from tables import *

import os
import zipfile
import gzip
import re
import logging

__CME_SRC_PATH__ = COMPRESSED_DATA_PATH / 'cme'

MsgType = '35'
SecurityDesc = '107'
SendingTime = '52'
MDUpdateAction = '279'
MDEntryType = '269'
MDPriceLevel = '1023'
MDEntryPx = '270'
MDEntrySize = '271'
NumberOfOrders = '346'
TradingSessionId = '336'
TickDirection = '274'
TradeVolume = '1020'
AggressorSide = '5797'

ActionNew = '0'
ActionChange = '1'
ActionDelete = '2'
ActionOverlay = '5'

BidEntryType = '0'
AskEntryType = '1'
TradeEntryType = '2'

tags = { 
    '75':'TradeDate',
    '268':'NoMDEntries',
    '107':'SecurityDesc',
    '279':'MDUpdateAction',
    '269':'MDEntryType',
    '286':'OpenCloseSettleFlag',
    '83':'RptSeq',
    '276':'QuoteCondition',
    '277':'TradeCondition',
    '1023':'MDPriceLevel',
    '273':'MDEntryTime',
    '271':'MDEntrySize',
    '270':'MDEntryPx',
    '346':'NumberOfOrders',
    '48':'SecurityID',
    '22':'SecurityIDSource',
    '336':'TradingSessionID',
    '274':'TickDirection',
    '451':'NetChgPrevDay',
    '1020':'TradeVolume',
    '1070':'MDQuoteType',
    '5797':'AggressorSide',
    '5799':'MatchEventIndicator'
}

inv_tags = {v:k for k, v in tags.items()}
    
def readable_record(record):
    return { tags.get(k,None):v for k,v in record.items() }

__BOOK_ENTRY_TYPES__ = [ BidEntryType, AskEntryType, TradeEntryType ]
        
class CmeBookBuilder(BookBuilder):
    def __init__(self, symbol, h5_file, **rest):
        BookBuilder.__init__(self, symbol, h5_file, **rest)

    def process_record(self, record):
        """
        Incorporate the contents of the new record into the bids/asks
        """
        ts = timestamp_from_cme_timestamp(record[SendingTime])
        entry_type = record.get(MDEntryType, None)
        if not entry_type in __BOOK_ENTRY_TYPES__:
            print "Skipping ", readable_record(record)
            return
        chicago_ts = record[SendingTime] + '=' + chicago_time_str(ts)
        recordType = record[MDUpdateAction]
        if recordType == ActionChange:
            is_bid = entry_type == BidEntryType
            print chicago_ts, "Update level", record[SecurityDesc], (is_bid and "Bid" or "Ask"), record[MDEntryPx], "qty", record[MDEntrySize], "LEV", record[MDPriceLevel]
        elif recordType == ActionNew:
            is_bid = entry_type == BidEntryType
            print chicago_ts, "New level", record[SecurityDesc], (is_bid and "Bid" or "Ask"), record.get(MDEntryPx,None), "by", record.get(MDEntrySize,None), "LEV", record.get(MDPriceLevel, None)
            if not record.get(MDEntryPx, None):
                print "Bogus ", readable_record(record)
        elif recordType == ActionDelete:
            is_bid = entry_type == BidEntryType
            print chicago_ts, "Delete level", record[SecurityDesc], (is_bid and "Bid" or "Ask"), record.get(MDEntryPx,None), "LEV", record.get(MDPriceLevel, None)
        elif recordType == ActionOverlay:
            pass
        else:
            print "INVALID RECORD:", str(record)
            raise RuntimeError("Invalid record: " + str(record))

        if record[MDEntryType] == '2':
            print "Trade", record[MDUpdateAction], record[MDEntryPx], record[MDEntrySize], "aggressor", (record[AggressorSide]=='1' and 'B' or 'S')

        # bids and asks have been updated, now update the record and append to the table
        # print "MsgType",record[MsgType]
        # print "MDUpdateAction",record[MDUpdateAction]
        # print "MDEntryType", record[MDEntryType]
        # print "MDEntryPx", record[MDEntryPx]
        # print "MDEntrySize", record[MDEntrySize]
        self.make_record(ts, chicago_ts)
        self._file_record_counter.increment_count()


class CmeFixParser(object):
    r"""

"""

    readable(input_paths=None) 

    match_all = re.compile(".*")

    def __init__(self, input_paths):
        """
        """
        self.__input_paths = input_paths
        self.__book_builders = {}
        self.__h5_file = None

    def advance_date(self, new_date):
        if self.__h5_file:
            self.__h5_file.close()
        output_path = __CME_SRC_PATH__ / 'h5' / str(new_date)
        if not output_path.parent.exists():
            os.makedirs(output_path.parent)
        print "OUT", output_path
        self.__h5_file = openFile(output_path, mode="w", title="CME Fix Data")
        self.__parse_manager = ParseManager(self.__current_input_path, self.__h5_file)

    def build_books(self, fields):
        if fields.get(MsgType, None) != 'X':
            return
        if len(fields) == 0:
            return

        symbol = fields.get(SecurityDesc, None)
        if not symbol:
            print "Missing symbol on", fields
            self.__parse_manager.warning(self.__current_file + ":: Missing symbol", 
                                         self.__line_number+1)
            
        try:
            symbol = fields[SecurityDesc]
            builder = self.__book_builders.get(symbol, None)
            if not builder:
                builder = CmeBookBuilder(symbol, self.__h5_file)
                self.__book_builders[symbol] = builder
        
            builder.process_record(fields)
        except Exception,e:
            print "Exception", e
            self.__parse_manager.warning(self.__current_file + ':' + str(symbol) +': ' + e.message, 
                                         self.__line_number+1)

    def parse(self):
        i = 0
        for zfile in self.input_paths:
            self.__current_input_path = zfile
            zfile = path(zfile)
            print "ZF", zfile
            date = get_date_of_file(zfile)
            self.advance_date(date)

            print "Processing zip file", zfile, "count", i
            if not zfile.exists():
                raise RuntimeError("Input path does not exist " + zfile)

            root = zipfile.ZipFile(zfile, 'r')
            files = root.namelist()
            files.sort()
            for f in files:
                print "Processing file", f, "count", i
                self.__line_number = 0
                self.__current_file = f
                for line in root.read(f).split("\n"):
                    #if i>2000000:
                    #    exit(0)
                    fields = {}
                    for field in line.split("")[0:-1]:
                        pair = field.split("=")
                        if len(pair) == 2:
                            fields[pair[0]] = pair[1]
                        else:
                            print "CRAP", pair

                    print readable_record(fields)
                    self.build_books(fields)
                    i =i+1

            print "Completed", i , "records"

if __name__ == "__main__":
    import pprint
    here = path(os.path.realpath(__file__))
    start = here.parent.parent.parent.parent  / 'data' / 'compressed'
    parser = CmeFixParser([ \
#            start / 'FFIX_20120212.zip', \
#            start / 'FFIX_20120213.zip', \
#            start / 'FFIX_20120214.zip' \
            start / 'FFIX_20111016.zip', \
            start / 'FFIX_20111017.zip', \
            start / 'FFIX_20111018.zip' \

                            ])
    parser.parse()
    pprint.pprint(vars(parser))

