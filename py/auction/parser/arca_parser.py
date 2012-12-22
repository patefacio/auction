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

import os
import zipfile
import re
import pprint

__PriceRe__ = re.compile(r"\s*(\d*)(?:\.(\d+))?\s*")

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
        else:
            raise RuntimeError("Invalid price format: " + pxStr)

class AddRecord(object):
    r"""
Single Add record
"""

    readable(symbol=None, price=None, quantity=None, orderId=None) 

    def __init__(self, fields):
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

class DeleteRecord(object):
    r"""
Single Delete record
"""

    readable(symbol=None, price=None, quantity=None, orderId=None) 

    def __init__(self, fields):
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

class ModifyRecord(object):
    r"""
Single Modify record
"""

    readable(symbol=None, price=None, quantity=None, orderId=None) 

    def __init__(self, fields):
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
    
class ArcaFixParser(object):
    r"""

Parse arca files and create book

"""

    readable(input_path=None, output_path=None, symbol_match_re=None) 

    def __init__(self, input_path, output_path, symbol_match_re = None):
        """
        input_path - path to input data
        output_path - path to folder containing output data
        """
        self.__input_path = input_path
        self.__output_path = output_path
        self.__symbol_match_re = symbol_match_re
        self.__active_orders = {}
        bookBuilders = {}

        if not self.__input_path.exists():
            raise RuntimeError("Input path does not exist " + self.__input_path)

        for index, line in enumerate(open(self.input_path)):
#            if index > 100:
#                break 

            fields = line.split(',')
            code = fields[0]
            record = None
            if code == 'A':
                record = AddRecord(fields)
            elif code == 'D':
                record = DeleteRecord(fields)
            elif code == 'M':
                record = ModifyRecord(fields)
            elif code == 'I':
                continue
            else:
                raise RuntimeError("Unexpected record type '" + 
                                   code + "' at line " + str(index) + 
                                   " of file " + self.__input_path)

            if self.symbol_match_re and not self.symbol_match_re.match(record.symbol):
                #print "Skipping ", record.symbol
                continue
            else:
                pass#print "Processing ", record.symbol

            builder = bookBuilders.get(record.symbol, None)
            if not builder:
                builder = BookBuilder(record.symbol)
                bookBuilders[record.symbol] = builder

            builder.processRecord(record)
#            print line
#            print vars(record)

        print "Processed ", index
        pprint.pprint(bookBuilders)
            

if __name__ == "__main__":
    import pprint
    here = path(os.path.realpath(__file__))
#    parser = ArcaFixParser(here.parent.parent.parent.parent / 'data' / 'uncompressed' / 'xau', 
    parser = ArcaFixParser(here.parent.parent.parent.parent / 'data' / 'uncompressed' / 'arcabookftp20070611.csv', 
                           None, re.compile(r"\bSPY\b"))
    pprint.pprint(vars(parser))

