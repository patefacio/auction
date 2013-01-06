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
from auction.book import Book

from tables import *
from sets import Set
import os
import zipfile
import re
import pprint
import logging
import gzip
import datetime
import time

__PriceRe__ = re.compile(r"\s*(\d*)(?:\.(\d+))?\s*")
__DateRe__ = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")

def getDateOfFile(fileName):
    year, month, day = __DateRe__.search(fileName).groups()
    return datetime.date(int(year), int(month), int(day))

def makeTimestamp(startOfDateSeconds, seconds, millis):
    seconds = int(seconds)
    millis = int(millis)
    return (startOfDateSeconds + seconds)*1000 + millis

def asciiTimestamp(ts):
    tmp = divmod(ts, 1000)
    millis = str(int(tmp[1])).rjust(3,'0')
    result = time.ctime(tmp[0])[-13:-5] + ':' + millis
    return result

# Given a price as a string converts to appropriate integer and decimal shift
def intPrice(pxStr):
    m = __PriceRe__.match(pxStr)
    if not m:
        raise RuntimeError("Invalid price " + pxStr)
    else:
        intPart, decimalPart = m.groups()
        if intPart == None or intPart == '':
            intPart = 0
        else:
            intPart = int(intPart)
        lenDecimalPart = decimalPart and len(decimalPart) or 0
        if lenDecimalPart == 4:
            return (4, intPart*10000+int(decimalPart))
        elif lenDecimalPart == 2:
            return (2, intPart*100+int(decimalPart))
        elif lenDecimalPart == 0:
            return (0, intPart)
        elif lenDecimalPart == 3:
            return (3, intPart*1000+int(decimalPart))
        elif lenDecimalPart == 1:
            return (1, intPart*10+int(decimalPart))
        elif lenDecimalPart == 6:
            return (6, intPart*1000000+int(decimalPart))
        elif lenDecimalPart == 5:
            return (5, intPart*100000+int(decimalPart))
        else:
            raise RuntimeError("Invalid price format: " + pxStr)

class AddRecord(object):
    r"""
Single Add record
"""

    readable(seqNum=None, orderId=None, exchange=None, isBuy=None,
             quantity=None, symbol=None, price=None, 
             systemCode=None, quoteId=None, timestamp=None)

    def __init__(self, fields, startOfDateSeconds):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seqNum = fields[1]
        self.__orderId = fields[2]
        self.__exchange = fields[3]
        self.__isBuy = fields[4] == 'B'
        self.__quantity = fields[5]
        self.__symbol = fields[6]
        self.__price = intPrice(fields[7])
        self.__systemCode = fields[10]
        self.__quoteId = fields[11]
        self.__timestamp = makeTimestamp(startOfDateSeconds, fields[8], fields[9])

class DeleteRecord(object):
    r"""
Single Delete record
"""

    readable(seqNum=None, orderId=None, symbol=None, 
             exchange=None, systemCode=None, quoteId=None,
             isBuy=None, timestamp=None) 

    def __init__(self, fields, startOfDateSeconds):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seqNum = fields[1]
        self.__orderId = fields[2]
        self.__symbol = fields[5]
        self.__exchange = fields[6]        
        self.__systemCode = fields[7]        
        self.__quoteId = fields[8]
        self.__isBuy = fields[9] == 'B'
        self.__timestamp = makeTimestamp(startOfDateSeconds, fields[3], fields[4])

class ModifyRecord(object):
    r"""
Single Modify record
"""

    readable(seqNum=None, orderId=None, quantity=None, price=None,
             symbol=None, quoteId=None, isBuy=None, timestamp=None) 

    def __init__(self, fields, startOfDateSeconds):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seqNum = fields[1]
        self.__orderId = fields[2]
        self.__quantity = fields[3]
        self.__price = intPrice(fields[4])
        self.__symbol = fields[7]  
        self.__quoteId = fields[10]
        self.__isBuy = fields[11] == 'B'
        self.__timestamp = makeTimestamp(startOfDateSeconds, fields[5], fields[6])

class BookBuilder(object):
    def __init__(self, symbol):
        self.__orders = {}

    def processRecord(self, record):
        if isinstance(record, AddRecord):
            pass#print "Adding record ", record.orderId
        elif isinstance(record, DeleteRecord):
            pass#print "Deleting record ", record.orderId        
        else:
            pass#print "Modifying record ", record.orderId

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
             matched_symbols = Set([]), date=None, startOfDateSeconds=None) 

    def __init__(self, input_path, output_path, symbol_match_re = None):
        """
        input_path - path to input data
        output_path - path to folder containing output data
        """
        self.__input_path = input_path
        self.__output_path = output_path
        self.__symbol_match_re = symbol_match_re
        self.__date = getDateOfFile(input_path.namebase)
        self.__startOfDateSeconds = time.mktime(self.date.timetuple())
        self.__active_orders = {}

        if not self.__input_path.exists():
            raise RuntimeError("Input path does not exist " + self.__input_path)

    def parse(self):
        print "Writing to ", self.__output_path
        h5file = openFile(self.__output_path, mode = "w", title = "Arca SPY")
        filters = Filters(complevel=1, complib='zlib')
        group = h5file.createGroup("/", 'SPY', 'Market book data')
        table = h5file.createTable(group, 'spy', ArcaRecord, "Foo", filters=filters)
        h5Record = table.row

        for index, line in enumerate(gzip.open(self.input_path, 'rb')):
#            if index > 10000000:
#                break 

            fields = line.split(',')
            code = fields[0]
            record = None
            if code == 'A':
                record = AddRecord(fields, self.startOfDateSeconds)
            elif code == 'D':
                record = DeleteRecord(fields, self.startOfDateSeconds)
            elif code == 'M':
                record = ModifyRecord(fields, self.startOfDateSeconds)
            elif code == 'I':
                continue
            else:
                raise RuntimeError("Unexpected record type '" + 
                                   code + "' at line " + str(index) + 
                                   " of file " + self.__input_path)

            if self.symbol_match_re and not self.symbol_match_re.match(record.symbol):
                continue

            self.matched_symbols.add(record.symbol)

            h5Record['ts'] = record.timestamp
            h5Record['asc_ts'] = asciiTimestamp(record.timestamp)
            h5Record['symbol'] = record.symbol
            h5Record['seq_num'] = record.seqNum
            h5Record['order_id'] = record.orderId
            h5Record['record_type'] = code
            h5Record['buy_sell'] = (record.isBuy and 'B' or 'S')
            if code != 'D':
                h5Record['price'] = record.price[1]
                h5Record['quantity'] = record.quantity

            h5Record.append()

            if 0 == index % 500:
                table.flush()

    def build_books(self):
        bookBuilders = {}
        builder = bookBuilders.get(record.symbol, None)
            # if not builder:
            #     builder = BookBuilder(record.symbol)
            #     bookBuilders[record.symbol] = builder

            # builder.processRecord(record)
#            print line
#            print vars(record)
        pprint.pprint(bookBuilders)
            

if __name__ == "__main__":
    import pprint
    import argparse

    parser = argparse.ArgumentParser("""
Take input raw data and generate corresponding hdf5 data files as well as book
files for symbols present in the raw data.
""")

    parser.add_argument('-d', '--date', 
                        dest='date',
                        action='store',
                        nargs='*',
                        help='Date(s) to process, if empty all dates assumed')

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

    compressed = COMPRESSED_DATA_PATH / 'arca'

    src_compressed_files = []
    if options.date:
        all_files = compressed.files()
        for date in options.date:
            src_compressed_files += filter(lambda f: f.find(date)>=0, all_files)
    else:
        src_compressed_files = compressed.files()

    symbolRe = re.compile(r"\bSPY\b")
    symbolRe = None
    
    for compressed_src in src_compressed_files:
        logging.info("Parsing fix file %s"%compressed_src)
        parser = ArcaFixParser(compressed_src, 
                               compressed / 'h5' / (str(getDateOfFile(compressed_src)) + '.h5'),
                               symbolRe)
        parser.parse()



