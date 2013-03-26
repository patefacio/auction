from path import path
from attribute import readable, writable
from tables import *
from auction.book import Book, BookTable
import sys

class RecordPointer(object):
    readable(symbol=0, index=0, count=0, current=None)
    
    def __init__(self, symbol, data_set):
        self.__symbol = symbol
        self.__data_set = data_set
        self.__count = data_set.nrows
        self.__index = 0
        self.__book = Book(data_set[0])
        self.__timestamp = self.__book.timestamp()

    def more(self):
        return self.__index < self.__count

    def timestamp(self):
        return self.__timestamp

    def next(self):
        if not self.__book:
            return None
        result = self.__book
        self.__index += 1
        if self.__index == self.__count:
            self.__book = None
            self.__timestamp = sys.maxint
        else:
            self.__book = Book(self.__data_set[self.__index])
            self.__timestamp = self.__book.timestamp()
        return result
    
    def __str__(self):
        return self.__symbol + ':' + str(self.timestamp())

class BookFileReader(object):
    r"""
A handle on an hdf file with standardized book data to read
"""

    readable(file_path=None, data_sets=[]) 

    def __init__(self, file_path, symbols):
        """

        """
        self.__book_file = openFile(file_path)
        self.__data_sets = []
        for symbol in symbols:
            for node in self.__book_file.root:
                if symbol == node._v_name:
                    self.__data_sets.append(RecordPointer(symbol, node.books))
                    break

    def ordered_visit(self, func):
        while True:
            self.__data_sets.sort(key=lambda r: r.timestamp())
            next_book = self.__data_sets[0].next()
            if not next_book:
                break
            func(self.__data_sets[0].symbol, next_book)
        

if __name__ == "__main__":
    import os
    import pprint
    from auction.paths import *    
    reader = BookFileReader(CME_OUT_PATH / '20111017', ['ESZ1'])
    count = 0

    def print_record(symbol, b):
        global count
        if (0 == (count%100000)):
            print count, symbol, b.timestamp_s(), b.top() 
        count += 1
        return

    reader.ordered_visit(print_record)
    
