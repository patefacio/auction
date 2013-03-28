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

import os
import zipfile
import gzip
import re
import logging
import sets
import string
import pprint
import traceback

__CME_SRC_PATH__ = DATA_PATH / 'CME_GLOBEX2'

__LEVELS__ = 10

ActionNew = '0'
ActionChange = '1'
ActionDelete = '2'
ActionOverlay = '5'

BidEntryType = '0'
AskEntryType = '1'
TradeEntryType = '2'

tags = { 
    '35':'MsgType',
    '1128':'ApplVerID',
    '49':'SenderCompID',
    '34':'MsgSeqNum',
    '43':'PosDupFlag',
    '52':'SendingTime',
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

repeated = sets.Set([
        'SecurityDesc',
        'MDUpdateAction',
        'MDEntryType',
        'OpenCloseSettleFlag',
        'RptSeq',
        'QuoteCondition',
        'TradeCondition',
        'MDPriceLevel',
        'MDEntryTime',
        'MDEntrySize',
        'MDEntryPx',
        'NumberOfOrders',
        'SecurityID',
        'SecurityIDSource',
        'TradingSessionID',
        'TickDirection',
        'NetChgPrevDay',
        'TradeVolume',
        'MDQuoteType',
        'AggressorSide',
        'MatchEventIndicator'])

repeated_tags = sets.Set([ inv_tags[r] for r in repeated ])
sorted_repeated = sorted(repeated)

MsgType = inv_tags['MsgType']
MsgSeqNum = inv_tags['MsgSeqNum']
NoMDEntries = inv_tags['NoMDEntries']
SecurityDesc = inv_tags['SecurityDesc']
SendingTime = inv_tags['SendingTime']
MDUpdateAction = inv_tags['MDUpdateAction']
MDEntryType = inv_tags['MDEntryType']
MDPriceLevel = inv_tags['MDPriceLevel']
MDEntryPx = inv_tags['MDEntryPx']
MDEntrySize = inv_tags['MDEntrySize']
NumberOfOrders = inv_tags['NumberOfOrders']
TradingSessionID = inv_tags['TradingSessionID']
TickDirection = inv_tags['TickDirection']
TradeVolume = inv_tags['TradeVolume']
AggressorSide = inv_tags['AggressorSide']
QuoteCondition = inv_tags['QuoteCondition']
TradeCondition = inv_tags['TradeCondition']

# print "All\n\t", string.join(sorted(tags.values()), "\n\t"), \
#     "\nrepeated\n\t", string.join(sorted_repeated, "\n\t"), \
#     "\nnon-repeated\n\t", string.join(sets.Set(tags.values()).difference(repeated),"\n\t")

def readable_update(record):
    r = { tags.get(k, k):v for k,v in record.items() }
    return pprint.pformat(r)


__BOOK_ENTRY_TYPES__ = [ BidEntryType, AskEntryType, TradeEntryType ]
        
class CmeBookBuilder(BookBuilder):

    readable(bid_book=None, ask_book=None)

    def __init__(self, symbol, h5_file, prior_day_books, **rest):
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

    def process_record(self, update, ts, chi_ts):
        """
        Incorporate the contents of the new record into the bids/asks
        """
        #print readable_update(update)
        #print pprint.pformat(update)
        entry_type = update[MDEntryType]

        action = update[MDUpdateAction]
        px = int(update.get(MDEntryPx, 0))
        qty = int(update.get(MDEntrySize, 0))
        level = int(update.get(MDPriceLevel, -1))-1

        if update.get(QuoteCondition, None):
            #print "Skipping", readable_update(update)
            return

        if update[MDEntryType] == TradeEntryType:
            if self._trade:
                self._trade['timestamp'] = ts
                self._trade['timestamp_s'] = chi_ts
                self._trade['price'] = px
                self._trade['quantity'] = qty
                side = update.get(AggressorSide)
                if side:
                    self._trade['trade_type'] = '1' if side=='1' else '2'
                else:
                    self._trade['trade_type'] = '3'
                self._trade.append()
                #print "Trade", action, px, qty,"aggressor", update.get(AggressorSide), \
                #    "tradecondition", update.get(TradeCondition)
        else:

            assert (level >= 0), "WARNING: bad level: %s"%readable_update(update)

            if action == ActionNew:
                is_bid = entry_type == BidEntryType
                if is_bid:
                    self.__bid_book[level:level] = [ (px, qty) ]
                    del self.__bid_book[__LEVELS__:]
                else:
                    self.__ask_book[level:level] = [ (px, qty) ]
                    del self.__ask_book[__LEVELS__:]

                # print ts, "Update level", update[SecurityDesc], \
                #     (is_bid and "Bid" or "Ask"), update[MDEntryPx], \
                #     "qty", update[MDEntrySize], "LEV", level

            elif action == ActionChange:
                is_bid = entry_type == BidEntryType
                if is_bid:
                    self.__bid_book[level] = (px, qty)
                else:
                    self.__ask_book[level] = (px, qty)

                # print ts, "New level", update[SecurityDesc], \
                #     (is_bid and "Bid" or "Ask"), update.get(MDEntryPx,None), \
                #     "by", update.get(MDEntrySize,None), "LEV", level

            elif action == ActionDelete:
                is_bid = entry_type == BidEntryType
                if is_bid:
                    del self.__bid_book[level]
                    self.__bid_book.append(None)
                else:
                    del self.__ask_book[level]
                    self.__ask_book.append(None)

                # print ts, "Delete level", update[SecurityDesc], \
                #     (is_bid and "Bid" or "Ask"), update.get(MDEntryPx,None), \
                #     "LEV", level

            elif action == ActionOverlay:
                pass
            else:
                print "INVALID UPDATE:", str(update)
                raise RuntimeError("Invalid update: " + str(update))

            assert(len(self.__bid_book) == __LEVELS__)
            assert(len(self.__ask_book) == __LEVELS__)

class CmeRefreshMessage(object):

    attribute(msg_type = None, msg_seq_num = None, sending_time = None, no_md_entries = 0) 
    readable(entries=None)

    assigner = {
        MsgType: lambda self, v: self.__setattr__('msg_type', v),
        MsgSeqNum: lambda self, v: self.__setattr__('msg_seq_num', v),
        SendingTime: lambda self, v: self.__setattr__('sending_time', v),
        NoMDEntries: lambda self, v: self.__setattr__('no_md_entries', int(v)),
        }

    def is_refresh_message(self):
        return self.msg_type == 'X'

    def __init__(self, line):
        self.__entries = []
        current = {}
        fields = line.split("")[0:-1]

        for field in fields:
            tag, value = field.split('=')
            action = CmeRefreshMessage.assigner.get(tag, None)
            if action != None:
                action(self, value)
            else:
                if tag in repeated_tags:
                    existing = current.get(tag, None)
                    if existing != None:
                        self.entries.append(current)
                        current = { tag : value}
                    else:
                        current[tag] = value
                else:
                    #print "Ignoring ", tag, tags.get(tag, None)
                    pass
         
        if len(current) > 0:
            self.entries.append(current)

        #print "Line:", line
        #print "NoMDEntries", self.no_md_entries, "entries", len(self.entries), self.entries
        assert((not self.is_refresh_message()) or self.no_md_entries == len(self.entries))

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
        self.__prior_day_books = {}
        self.__h5_file = None
        self.__ts = 0
        self.__chi_ts = None
        self.__data_start_timestamp = 0
        self.__output_path = None

    def write_summary(self):
        ############################################################
        # Finish filling in the parse summary info and close up
        ############################################################
        self.__parse_manager.data_start(self.__data_start_timestamp)
        self.__parse_manager.data_stop(self.__ts)
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
        self.__ts = 0
        for symbol, builder in self.__book_builders.items():
            self.__prior_day_books[symbol] = (builder.bid_book, builder.ask_book)
        self.__book_builders = {}

    def build_books(self, msg):
        try:
            ts = timestamp_from_cme_timestamp(msg.sending_time)
            if self.__ts:
                if ts < self.__ts:
                    print "At", self.__line_number+1, "of", self.__current_file, \
                        "previous ts:", self.__chi_ts, "new:", chi_ts, \
                        "Current Message:", pprint.pformat(msg)
                    assert False, "Timestamps going backward"

            self.__ts = ts
            self.__chi_ts = chicago_time_str(self.__ts)
            if 0 == self.__data_start_timestamp:
                self.__data_start_timestamp = self.__ts
            affected_builders = sets.Set()
            for update in msg.entries:
                symbol = update[SecurityDesc]
                builder = self.__book_builders.get(symbol, None)
                if not builder:
                    builder = CmeBookBuilder(symbol, self.__h5_file, 
                                             self.__prior_day_books.get(symbol, None),
                                             include_trades = True)
                    self.__book_builders[symbol] = builder

                if not update[MDEntryType] in __BOOK_ENTRY_TYPES__:
                    continue
        
                builder.process_record(update, self.__ts, self.__chi_ts)
                affected_builders.add(builder)

            for builder in affected_builders:
                top_bid = builder.top_bid()
                top_ask = builder.top_ask()
                if top_bid and top_ask:
                    if top_bid == top_ask:
                        msg = builder.symbol + ': Locked (%s, %s)'%(top_bid, top_ask)
                        #print msg
                        self.__parse_manager.warning(msg, 'L', self.__ts, self.__line_number+1)
                    elif top_bid > top_ask:
                        msg = builder.symbol + ': Crossed (%s, %s)'%(top_bid, top_ask)
                        #print msg
                        self.__parse_manager.warning(msg, 'C', self.__ts, self.__line_number+1)

                    builder.write_record(self.__ts, self.__chi_ts)

        except Exception,e:
            print traceback.format_exc()
            self.__parse_manager.warning(self.__current_file + ':' + e.message, 
                                         'G', self.__ts,
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
            files.reverse()
            for f in files:
                print "Processing file", f, "count", i
                self.__line_number = 0
                self.__current_file = f
                for line in root.read(f).split("\n"):
                    i =i+1
                    msg = CmeRefreshMessage(line)
                    if not msg.is_refresh_message():
                        continue
                    self.build_books(msg)
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
                        action='store',
                        nargs='+',
                        help='Date(s) to process, if empty all dates assumed')

    parser.add_argument('-v', '--verbose', 
                        dest='verbose',
                        action='store_true',
                        help='Output extra logging information')

    options = parser.parse_args()

    if options.verbose:
        logging.basicConfig(level=logging.INFO)

    print "Processing dates:", options.dates
    here = path(os.path.realpath(__file__))

    start = __CME_SRC_PATH__

    fileset = CmeRawFileSet()

    files = fileset.get_files(options.dates)
    assert len(files) == len(options.dates)
    print "Files are", pprint.pformat(files)


    parser = CmeFixParser(files)
    parser.parse()
    pprint.pprint(vars(parser))

