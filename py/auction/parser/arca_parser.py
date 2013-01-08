###############################################################################
#
# File: arca_parser.py
#
# Copyright (c) 2012 by First Class Software, Inc.
#
# All Rights Reserved. 
#
# Quote: There are only two kinds of programming languages: those people
# always bitch about and those nobody uses. (Bjarne Stroustrup)
#
# Description: Parser for arca record data files
#
# History:
#
# date       user       comment
# --------   --------   ------------------------------------------------------
# 12/20/12   dbdavidson Initial Creation.
#
##############################################################################
from path import path
from attribute import readable, writable
from auction.paths import *
from auction.book import Book, BookTable

from tables import *
from sets import Set
import os
import zipfile
import re
import logging
import gzip
import datetime
import time
import string
import pprint
from numpy import zeros, array

__PriceRe__ = re.compile(r"\s*(\d*)(?:\.(\d+))?\s*")
__DateRe__ = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")
__ARCA_SRC_PATH__ = COMPRESSED_DATA_PATH / 'arca'
__FLUSH_FREQ__ = 1000
__LEVELS__ = 5

class PriceOrderedDict(object):
    def __init__(self, ascending = True):
        self.d = {}
        self.L = []
        self.ascending = ascending
        self.sorted = True
    
    def __len__(self):
        return len(self.L)

    def get_quantity(self, px):
        return self.d.get(px, 0)

    def update_quantity(self, px, q):
        assert(q!=0)
        assert(len(self.L) == len(self.d))
        qty = self.d.get(px)
        if qty:
            qty += q
            if qty:
                self.d[px] = qty
            else:
                del self.d[px]
                self.L.remove(px)
        else:
            self.L.append(px)
            self.sorted = False
            self.d[px] = q

    def top(self):
        if not self.sorted:
            self.L.sort()
            self.sorted = True
        if 0 == len(self.L):
            return None
        if self.ascending:
            return self.L[0]
        else:
            return self.L[-1]

def get_date_of_file(fileName):
    m = __DateRe__.search(fileName).groups()
    if m:
        year, month, day = m
        return datetime.date(int(year), int(month), int(day))
    else:
        return None

def make_timestamp(start_of_date_seconds, seconds, millis):
    seconds = int(seconds)
    millis = int(millis)
    return (start_of_date_seconds + seconds)*1000 + millis

def ascii_timestamp(ts):
    tmp = divmod(ts, 1000)
    millis = str(int(tmp[1])).rjust(3,'0')
    result = time.ctime(tmp[0])[-13:-5] + ':' + millis
    return result

# Given a price as a string converts to appropriate integer and decimal shift
def int_price(px_str):
    m = __PriceRe__.match(px_str)
    if not m:
        raise RuntimeError("Invalid price " + px_str)
    else:
        int_part, decimal_part = m.groups()
        if int_part == None or int_part == '':
            int_part = 0
        else:
            int_part = int(int_part)

        len_decimal_part = decimal_part and len(decimal_part) or 0
        if len_decimal_part == 4:
            return (4, int_part*10000+int(decimal_part))
        elif len_decimal_part == 2:
            return (2, int_part*100+int(decimal_part))
        elif len_decimal_part == 0:
            return (0, int_part)
        elif len_decimal_part == 3:
            return (3, int_part*1000+int(decimal_part))
        elif len_decimal_part == 1:
            return (1, int_part*10+int(decimal_part))
        elif len_decimal_part == 6:
            return (6, int_part*1000000+int(decimal_part))
        elif len_decimal_part == 5:
            return (5, int_part*100000+int(decimal_part))
        else:
            raise RuntimeError("Invalid price format: " + px_str)

class AddRecord(object):
    r"""
Single Add record
"""

    readable(seq_num=None, order_id=None, exchange=None, is_buy=None,
             quantity=None, symbol=None, price=None, 
             system_code=None, quote_id=None, timestamp=None)

    def __init__(self, fields, start_of_date_seconds):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seq_num = fields[1]
        self.__order_id = fields[2]
        self.__exchange = fields[3]
        self.__is_buy = fields[4] == 'B'
        self.__quantity = int(fields[5])
        self.__symbol = fields[6]
        self.__price = int_price(fields[7])
        self.__system_code = fields[10]
        self.__quote_id = fields[11]
        self.__timestamp = make_timestamp(start_of_date_seconds, fields[8], fields[9])

class DeleteRecord(object):
    r"""
Single Delete record
"""

    readable(seq_num=None, order_id=None, symbol=None, 
             exchange=None, system_code=None, quote_id=None,
             is_buy=None, timestamp=None) 

    def __init__(self, fields, start_of_date_seconds):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seq_num = fields[1]
        self.__order_id = fields[2]
        self.__symbol = fields[5]
        self.__exchange = fields[6]        
        self.__system_code = fields[7]        
        self.__quote_id = fields[8]
        self.__is_buy = fields[9] == 'B'
        self.__timestamp = make_timestamp(start_of_date_seconds, fields[3], fields[4])

class ModifyRecord(object):
    r"""
Single Modify record
"""
    readable(seq_num=None, order_id=None, quantity=None, price=None,
             symbol=None, quote_id=None, is_buy=None, timestamp=None) 

    def __init__(self, fields, start_of_date_seconds):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seq_num = fields[1]
        self.__order_id = fields[2]
        self.__quantity = int(fields[3])
        self.__price = int_price(fields[4])
        self.__symbol = fields[7]  
        self.__quote_id = fields[10]
        self.__is_buy = fields[11] == 'B'
        self.__timestamp = make_timestamp(start_of_date_seconds, fields[5], fields[6])

class FileRecordCounter(object):
    readable(h5_file = None, count = 0)

    def __init__(self, h5_file):
        self.__h5_file = h5_file
        self.__count = 0

    def increment_count(self):
        self.__count += 1
        print "Incrementing counter ", self.__count
        if 0 == (self.__count % __FLUSH_FREQ__):
            print "Calling flush"
            self.__h5_file.flush()

class BookBuilder(object):

    __book_files__ = {}

    def __init__(self, symbol, file_name):
        self.__output_path = BOOK_DATA / file_name
        self.__file_record_counter = BookBuilder.__book_files__.get(self.__output_path, None)
        if not self.__file_record_counter:
            h5_file = openFile(self.__output_path, mode = "w", title = "Book data")
            BookBuilder.__book_files__[self.__output_path] = FileRecordCounter(h5_file)
            self.__file_record_counter = BookBuilder.__book_files__[self.__output_path]

        h5_file = self.__file_record_counter.h5_file
        filters = Filters(complevel=1, complib='zlib')
        group = h5_file.createGroup("/", symbol, 'Book data')
        self.__table = h5_file.createTable(group, 'books', BookTable, 
                                           "Data for "+str(symbol), filters=filters)
        self.__tick_size = 100 # TODO
        self.__record = self.__table.row
        self.__symbol = symbol
        self.__orders = {}
        self.__bids_to_qty = PriceOrderedDict(False)
        self.__asks_to_qty = PriceOrderedDict()
        self.__bids = zeros(shape=[__LEVELS__,2])
        self.__asks = zeros(shape=[__LEVELS__,2])

    def summary(self):
        print "Completed data for:", self.__symbol
        print "\tOutstanding orders:", len(self.__orders)
        print "\tOutstanding bids:", len(self.__bids_to_qty)
        print "\tOutstanding asks:", len(self.__asks_to_qty)


    def make_record(self, ts, ts_s):
        bidTopPx = self.__bids_to_qty.top()
        if bidTopPx:
            for i, px in enumerate(range(bidTopPx, 
                                         bidTopPx-__LEVELS__*self.__tick_size, 
                                         -self.__tick_size)):
                self.__bids[i][0] = px
                self.__bids[i][1] = self.__bids_to_qty.get_quantity(px)
        else:
            self.__bids = zeros(shape=[__LEVELS__,2])

        askTopPx = self.__asks_to_qty.top()
        if askTopPx:
            for i, px in enumerate(range(askTopPx, 
                                         askTopPx+__LEVELS__*self.__tick_size, 
                                         self.__tick_size)):
                self.__asks[i][0] = px
                self.__asks[i][1] = self.__asks_to_qty.get_quantity(px)
        else:
            self.__asks = zeros(shape=[__LEVELS__,2])

        self.__record['bid'] = self.__bids
        self.__record['ask'] = self.__asks
        self.__record['timestamp'] = ts
        self.__record['timestamp_s'] = ts_s

        if self.__bids[0][0] and self.__asks[0][0] and (self.__bids[0][0] > self.__asks[0][0]):
            msg = ["Encountered Crossed Market", 
                   "Bids: "+str(self.__bids), 
                   "Asks: "+str(self.__asks)]
            raise RuntimeError(string.join(msg, "\n\t"))
        

    def process_record(self, amd_record):

        if isinstance(amd_record, AddRecord):
            entry = (amd_record.price[1], amd_record.quantity)
            current = self.__orders.setdefault(amd_record.order_id, entry)
            if current != entry:
                raise RuntimeError("Duplicate add for order: " + amd_record.order_id)

            if amd_record.is_buy:
                self.__bids_to_qty.update_quantity(entry[0], entry[1])
            else:
                self.__asks_to_qty.update_quantity(entry[0], entry[1])

        elif isinstance(amd_record, DeleteRecord):
            assert(amd_record.symbol == self.__symbol)
            current = self.__orders.get(amd_record.order_id, None)
            if not current:
                raise "Record not found for delete: " + amd_record.order_id

            if amd_record.is_buy:
                self.__bids_to_qty.update_quantity(current[0], -current[1])
            else:
                self.__asks_to_qty.update_quantity(current[0], -current[1])

            del self.__orders[amd_record.order_id]

        elif isinstance(amd_record, ModifyRecord):
            assert(amd_record.symbol == self.__symbol)
            current = self.__orders.get(amd_record.order_id, None)
            if not current:
                raise "Record not found for modify: " + amd_record.order_id

            if amd_record.is_buy:
                self.__bids_to_qty.update_quantity(current[0], -current[1])
                self.__bids_to_qty.update_quantity(amd_record.price[1], amd_record.quantity)
            else:
                self.__asks_to_qty.update_quantity(current[0], -current[1])
                self.__asks_to_qty.update_quantity(amd_record.price[1], amd_record.quantity)

            self.__orders[amd_record.order_id] = (amd_record.price[1], amd_record.quantity)
        else:
            raise RuntimeError("Invalid record: " + amd_record)

        self.make_record(amd_record.timestamp, ascii_timestamp(amd_record.timestamp))
        self.__record.append()
        self.__file_record_counter.increment_count()

class ArcaRecord(IsDescription):
    asc_ts      = StringCol(12)
    ts          = Int64Col()
    seq_num     = Int64Col()
    order_id    = Int64Col()
    symbol      = StringCol(8) 
    price       = Int64Col()
    quantity    = Int32Col()
    record_type = StringCol(1) # 'A', 'M', 'D'
    buy_sell    = StringCol(1) # 'B', 'S'
    
class ArcaFixParser(object):
    r"""

Parse arca files and create book

"""

    readable(input_path=None, output_path=None, symbol_match_re=None,
             matched_symbols = Set([]), date=None, start_of_date_seconds=None,
             book_builders=None) 

    def __init__(self, input_path, input_date, file_tag, symbol_match_re = None):
        """
        input_path - path to input data
        input_date - date of data in file
        file_tag - tag for naming the file
        symbol_match_re - regex on which symbols to include
        """
        self.__input_path = input_path
        self.__date = input_date
        self.__file_tag = file_tag
        self.__output_path = __ARCA_SRC_PATH__ / 'h5' / (str(self.__date) + file_tag + '.h5')
        self.__symbol_match_re = symbol_match_re
        self.__start_of_date_seconds = time.mktime(self.__date.timetuple())
        self.__book_builders = {}

        logging.info("Parsing file %s to create %s"% (self.__input_path, self.__output_path))

        if not self.__input_path.exists():
            raise RuntimeError("Input path does not exist " + self.__input_path)

    def parse(self, build_book = True):
        print "Writing to ", self.__output_path
        h5file = openFile(self.__output_path, mode = "w", title = "ARCA Equity Data")
        filters = Filters(complevel=1, complib='zlib')
        group = h5file.createGroup("/", 'AMD', 'Add-Modify-Delete data')
        table = h5file.createTable(group, 'records', ArcaRecord, 
                                   "Data for "+str(self.date), filters=filters)
        h5Record = table.row
        hitCount = 0

        for index, line in enumerate(gzip.open(self.input_path, 'rb')):
            # if hitCount > 10000:
            #     print "Done for now..."
            #     pprint.pprint(self)
            #     break 

            if 0 == (index % 1000000):
                logging.info("At %d hit count is %d on %s" % 
                             (index, hitCount, 
                              (self.symbol_match_re and self.symbol_match_re.pattern or "*")))

            fields = line.split(',')
            code = fields[0]
            record = None
            if code == 'A':
                record = AddRecord(fields, self.start_of_date_seconds)
            elif code == 'D':
                record = DeleteRecord(fields, self.start_of_date_seconds)
            elif code == 'M':
                record = ModifyRecord(fields, self.start_of_date_seconds)
            elif code == 'I':
                continue
            else:
                raise RuntimeError("Unexpected record type '" + 
                                   code + "' at line " + str(index) + 
                                   " of file " + self.__input_path)

            if self.symbol_match_re and not self.symbol_match_re.search(record.symbol):
                continue
            else:
                hitCount += 1

                self.matched_symbols.add(record.symbol)

                if build_book:
                    self.build_books(record)
                else:
                    h5Record['ts'] = record.timestamp
                    h5Record['asc_ts'] = ascii_timestamp(record.timestamp)
                    h5Record['symbol'] = record.symbol
                    h5Record['seq_num'] = record.seq_num
                    h5Record['order_id'] = record.order_id
                    h5Record['record_type'] = code
                    h5Record['buy_sell'] = (record.is_buy and 'B' or 'S')
                    if code != 'D':
                        h5Record['price'] = record.price[1]
                        h5Record['quantity'] = record.quantity

                    h5Record.append()

                    if 0 == index % __FLUSH_FREQ__:
                        table.flush()

        for symbol, builder in self.book_builders.iteritems():
            builder.summary()

    def build_books(self, record):
        builder = self.book_builders.get(record.symbol, None)
        if not builder:
            builder = BookBuilder(record.symbol, "book_" + self.__file_tag + ".h5")
            self.book_builders[record.symbol] = builder

        builder.process_record(record)
            

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("""
Take input raw data and generate corresponding hdf5 data files as well as book
files for symbols present in the raw data.
""")

    parser.add_argument('-d', '--date', 
                        dest='dates',
                        action='store',
                        nargs='*',
                        help='Date(s) to process, if empty all dates assumed')

    parser.add_argument('-s', '--symbol', 
                        dest='symbols',
                        action='store',
                        nargs='*',
                        help='Symbols to include')

    parser.add_argument('-f', '--force', 
                        dest='force',
                        action='store_true',
                        help='Overwrite existing files')

    parser.add_argument('-v', '--verbose', 
                        dest='verbose',
                        action='store_true',
                        help='Output extra logging information')

    options = parser.parse_args()

    if options.verbose:
        logging.basicConfig(level=logging.INFO)

    src_compressed_files = []
    if options.dates:
        all_files = __ARCA_SRC_PATH__.files()
        for date in options.dates:
            src_compressed_files += filter(lambda f: f.find(date)>=0, all_files)
    else:
        src_compressed_files = __ARCA_SRC_PATH__.files()

    if options.symbols:
        reText = r'\b(?:' + string.join(options.symbols, '|') + r')\b'
        symbol_text = '_' + re.sub(r'\W', '.', reText) + '_'
        symbol_re = re.compile(reText)
    else:
        symbol_text = '_ALL_'
        symbol_re = None

    for compressed_src in src_compressed_files:
        date = get_date_of_file(compressed_src)
        if date:
        #if compressed_src.find('spy') < 0:
        #    continue
            parser = ArcaFixParser(compressed_src, date, symbol_text, symbol_re)
            parser.parse()
