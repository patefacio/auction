###############################################################################
#
# File: arca_parser.py
#
# Description: Parser for arca record data files
#
##############################################################################
from path import path
from attribute import readable, writable
from auction.paths import *
from auction.book import Book, BookTable
from auction.parser.parser_summary import ParseManager
from auction.time_utils import *

from tables import *
from sets import Set
from auction.parser.utils import PriceOrderedDict, FileRecordCounter, BookBuilder
import os
import zipfile
import re
import logging
import gzip
import datetime
import time
import string
import pprint
import traceback
from numpy import zeros, array

__PriceRe__ = re.compile(r"\s*(\d*)(?:\.(\d+))?\s*")
__DateRe__ = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")
__ARCA_SRC_PATH__ = DATA_PATH / 'NYSE_ARCA2'
__ARCA_OUT_PATH__ = BOOK_DATA / 'arca'
__FLUSH_FREQ__ = 10000
__LEVELS__ = 10
__PX_MULTIPLIER__ = 1000000
__PX_DECIMAL_DIGITS__ = 6
__TICK_SIZE__ = 10000

def get_date_of_file(fileName):
    """
    Given a filename with a date in it (YYYYMMDD), parse out the date
    Return None if no date present
    """
    m = __DateRe__.search(fileName)
    if m:
        year, month, day = m.groups()
        return datetime.date(int(year), int(month), int(day))
    else:
        return None

def make_timestamp(start_of_date, seconds, millis):
    """
    Given a timestamp for start_of_date, calculate new timestamp given
    additional seconds and millis.
    """
    seconds = int(seconds)
    try:
        millis = 0 if millis=='' else int(millis)
    except Exception,e:
        print "Invalid millis:", millis
        raise e
        
    return start_of_date + seconds*1000000 + millis*1000

def int_price(px_str):
    """
    Given a price as a string, convert to an integer. All prices are stored as
    64bit integers. The largest decimal I've encountered is 6 places, so all
    prices are aligned to 6 decimals. Given an integer price px, then, the
    quoted price would be px/1e6
    """
    m = __PriceRe__.match(px_str)
    if not m:
        raise RuntimeError("Invalid price " + px_str)
    else:
        int_part, decimal_part = m.groups()

        if int_part == None or int_part == '':
            int_part = 0
        else:
            int_part = int(int_part)

        if not decimal_part:
            decimal_part = "0"
        
        if len(decimal_part) > 6:
            raise RuntimeError("Invalid price - more than 6 decimal places:" + px_str)

        return int_part*__PX_MULTIPLIER__ + \
            int(decimal_part.ljust(__PX_DECIMAL_DIGITS__, '0'))

class AddRecord(object):
    r"""
Stores fields for single Add record
"""

    readable(seq_num=None, order_id=None, exchange=None, is_buy=None,
             quantity=None, symbol=None, price=None, 
             system_code=None, quote_id=None, timestamp=None)

    def __init__(self, fields, start_of_date):
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
        self.__timestamp = make_timestamp(start_of_date, fields[8], fields[9])

class DeleteRecord(object):
    r"""
Stores fields for single Delete record
"""

    readable(seq_num=None, order_id=None, symbol=None, 
             exchange=None, system_code=None, quote_id=None,
             is_buy=None, timestamp=None) 

    def __init__(self, fields, start_of_date):
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
        self.__timestamp = make_timestamp(start_of_date, fields[3], fields[4])

class ModifyRecord(object):
    r"""
Stores fields for single Modify record
"""
    readable(seq_num=None, order_id=None, quantity=None, price=None,
             symbol=None, quote_id=None, is_buy=None, timestamp=None) 

    def __init__(self, fields, start_of_date):
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
        self.__timestamp = make_timestamp(start_of_date, fields[5], fields[6])


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

class ArcaBookBuilder(BookBuilder):
    def __init__(self, symbol, h5_file, **rest):
        BookBuilder.__init__(self, symbol, h5_file, **rest)
        self._bid_orders = {}
        self._ask_orders = {}

    def hanging_orders(self):
        return len(self._bid_orders) + len(self._ask_orders)

    def process_record(self, amd_record):
        """
        Incorporate the contents of the new record into the bids/asks
        """

        is_buy = amd_record.is_buy
        orders = self._bid_orders if is_buy else self._ask_orders
        side = "B" if is_buy else "A"

        if isinstance(amd_record, AddRecord):
            entry = (amd_record.price, amd_record.quantity)
            #current = orders.setdefault(amd_record.order_id, entry)
            current = orders.get(amd_record.order_id)
            if not current:
                orders[amd_record.order_id] = entry
            else:
                #print "Dealing with multi add", self._symbol, side, amd_record.order_id
                # Don't raise an error - turn the entry into a list if it is not
                if type(current) != list:
                    orders[amd_record.order_id] = [current]

                # Put this new add at front of list so pop effects FIFO
                #orders[amd_record.order_id].insert(0, entry)
                orders[amd_record.order_id].append(entry)

            if amd_record.is_buy:
                self._bids_to_qty.update_quantity(entry[0], entry[1])
            else:
                self._asks_to_qty.update_quantity(entry[0], entry[1])

        elif isinstance(amd_record, DeleteRecord):
            assert(amd_record.symbol == self._symbol)
            current = orders.get(amd_record.order_id, None)
            if not current:
                raise RuntimeError("Record not found for delete: " + amd_record.order_id)

            is_list = (type(current) == list)
            if is_list:
                # If it is a list, pop off the first entry (FIFO) and "delete" that qty
                #print "Deleting", amd_record.order_id, "one elm of list", side, orders,
                original_list = current
                current = orders[amd_record.order_id].pop()
                #print "with current volume at", current[0], (self._bids_to_qty.get_quantity(current[0]) if amd_record.is_buy else \
                #                                                 self._asks_to_qty.get_quantity(current[0]))

            if amd_record.is_buy:
                self._bids_to_qty.update_quantity(current[0], -current[1])
            else:
                self._asks_to_qty.update_quantity(current[0], -current[1])

            if is_list:
                if len(original_list) == 0:
                    #print "Deleting multi-order list", amd_record.order_id
                    del orders[amd_record.order_id]
            else:
                del orders[amd_record.order_id]

        elif isinstance(amd_record, ModifyRecord):
            assert(amd_record.symbol == self._symbol)
            current = orders.get(amd_record.order_id)

            if not current:
                raise RuntimeError("Record not found for modify: " + amd_record.order_id)

            if type(current) == list:
                print "UNABLE TO SUPPORT MODIFY with duplicate adds!", amd_record.order_id, "mod", amd_record, "current", current
                raise RuntimeError("Record not found for modify: " + amd_record.order_id)

            if amd_record.is_buy:
                self._bids_to_qty.update_quantity(current[0], -current[1])
                self._bids_to_qty.update_quantity(amd_record.price, amd_record.quantity)
            else:
                self._asks_to_qty.update_quantity(current[0], -current[1])
                self._asks_to_qty.update_quantity(amd_record.price, amd_record.quantity)

            orders[amd_record.order_id] = (amd_record.price, amd_record.quantity)
        else:
            raise RuntimeError("Invalid record: " + amd_record)

        # bids and asks have been updated, now update the record and append to the table
        self.make_record(amd_record.timestamp, 
                         chicago_time_str(amd_record.timestamp))

    
class ArcaFixParser(object):
    r"""

Parse arca files and create book

"""


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
        self.__output_base = __ARCA_OUT_PATH__ / (str(self.__date)+'_'+file_tag)
        self.__symbol_match_re = symbol_match_re
        self.__start_of_date = start_of_date(self.__date.year, self.__date.month,
                                             self.__date.day, NY_TZ)

        self.__book_builders = {}

        if not self.__input_path.exists():
            raise RuntimeError("Input path does not exist " + self.__input_path)

    def parse(self, build_book = True, force = False, stop_early_at_hit=0):
        """
        Parse the input file. There are two modes: build_book=True and
        build_book=False. If build_book=False, the h5 file is simply the same
        record data from the gz file, but stored as hdf5. If build_book=True,
        the hdf5 file created has book data for all matching inputs. Each
        symbol gets it's own dataset.

        The ParseManager is used to store summary information for the parse of
        this data.
        """
        self.__output_path = self.__output_base + (build_book and ".h5" or "_AMD_.h5")
        logging.info("Parsing file %s\n\tto create %s"% (self.__input_path, self.__output_path))
        if self.__output_path.exists() and not force:
            return
        if not self.__output_path.parent.exists():
            os.makedirs(self.__output_path.parent)
        self.__h5_file = openFile(self.__output_path, mode = "w", title = "ARCA Equity Data")
        if not build_book:
            ## If not building book, then just writing out AMD data as hdf5
            filters = Filters(complevel=1, complib='zlib')
            group = self.__h5_file.createGroup("/", '_AMD_Data_', 'Add-Modify-Delete data')
            table = self.__h5_file.createTable(group, 'records', ArcaRecord, 
                                               "Data for "+str(self.__date), filters=filters)
            h5Record = table.row

        self.__parse_manager = ParseManager(self.__input_path, self.__h5_file)
        self.__parse_manager.mark_start()

        hit_count = 0
        data_start_timestamp = None

        for self.__line_number, line in enumerate(gzip.open(self.__input_path, 'rb')):

            if stop_early_at_hit and hit_count == stop_early_at_hit:
                break 

            ###################################################
            # Show progress periodically
            ###################################################
            if 0 == (self.__line_number % 1000000):
                logging.info("At %d hit count is %d on %s" % 
                             (self.__line_number, hit_count, 
                              (self.__symbol_match_re and 
                               self.__symbol_match_re.pattern or "*")))

            fields = re.split(r'\s*,\s*', line)
            code = fields[0]
            record = None
            if code == 'A':
                record = AddRecord(fields, self.__start_of_date)
            elif code == 'D':
                record = DeleteRecord(fields, self.__start_of_date)
            elif code == 'M':
                record = ModifyRecord(fields, self.__start_of_date)
            elif code == 'I':
                continue
            else:
                raise RuntimeError("Unexpected record type '" + 
                                   code + "' at line " + str(self.__line_number) + 
                                   " of file " + self.__input_path)

            if self.__symbol_match_re and \
                    not self.__symbol_match_re.search(record.symbol):
                continue
            else:
                hit_count += 1

                # record the timestamp of the first record as data_start
                if not data_start_timestamp:
                    data_start_timestamp = record.timestamp

                if build_book:
                    self.build_books(record)
                else:
                    h5Record['ts'] = record.timestamp
                    h5Record['asc_ts'] = chicago_time_str(record.timestamp)
                    h5Record['symbol'] = record.symbol
                    h5Record['seq_num'] = record.seq_num
                    h5Record['order_id'] = record.order_id
                    h5Record['record_type'] = code
                    h5Record['buy_sell'] = (record.is_buy and 'B' or 'S')
                    if code != 'D':
                        h5Record['price'] = record.price
                        h5Record['quantity'] = record.quantity

                    h5Record.append()

                    if 0 == self.__line_number % __FLUSH_FREQ__:
                        table.flush()

        books_good = True
        total_unchanged = 0
        for symbol, builder in self.__book_builders.iteritems():
            books_good = books_good and builder.summary()
            total_unchanged += builder.unchanged

        ############################################################
        # Finish filling in the parse summary info and close up
        ############################################################
        self.__parse_manager.data_start(data_start_timestamp)
        self.__parse_manager.data_stop(record.timestamp)
        self.__parse_manager.irrelevants(total_unchanged)
        self.__parse_manager.processed(self.__line_number+1)
        self.__parse_manager.mark_stop(books_good)
        self.__h5_file.close()
        ParseManager.summarize_file(self.__output_path)


    def build_books(self, record):
        """
        Dispatch the new record to the appropriate BookBuilder for the symbol
        """
        builder = self.__book_builders.get(record.symbol, None)
        if not builder:
            builder = ArcaBookBuilder(record.symbol, self.__h5_file)
            self.__book_builders[record.symbol] = builder

        try:
            builder.process_record(record)
        except Exception,e:
            #print traceback.format_exc()
            self.__parse_manager.warning(record.symbol +': ' + e.message, self.__line_number+1)
            

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

    symbol_text = None
    if not options.symbols:
        options.symbols = [ 
            # Index ETFs
            'SPY', 'DIA', 'QQQ', 
            # Sectors
            'XLK', 'XLF', 'XLP', 'XLE', 'XLY', 'XLV', 'XLB',
            # Vanguard
            'VCR', 'VDC', 'VHT', 'VIS', 'VAW', 'VNQ', 'VGT', 'VOX', 'VPU',
            # Energy
            'XOM', 'RDS', 'BP',
            # Home Improvement
            'HD', 'LOW', 'XHB', 
            # Banks
            'MS', 'GS', 'BAC', 'JPM', 'C', 
            # Exchanges
            'CME', 'NYX',
            # Big Techs
            'AAPL', 'MSFT', 'GOOG', 'CSCO'
            ]
        symbol_text = 'MOTLEY_10Lev'

    options.symbols.sort()
    re_text = r'\b(?:' + string.join(options.symbols, '|') + r')\b'
    symbol_re = re.compile(re_text)
    if not symbol_text:
        symbol_text = string.join(options.symbols, '_')

    for compressed_src in src_compressed_files:
        date = get_date_of_file(compressed_src)
        print "Examining", compressed_src.basename(), date
        if date:
            parser = ArcaFixParser(compressed_src, date, symbol_text, symbol_re)
            #parser.parse(True, 50000)
            parser.parse(True, False)
