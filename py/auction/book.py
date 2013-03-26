from tables import *
import string

class BookTable(IsDescription):
    """
    Basic book table
    """
    timestamp   = Int64Col()
    timestamp_s = StringCol(16)
    ask         = Int64Col(shape=(10,2))
    bid         = Int64Col(shape=(10,2))
    seqnum      = Int64Col()

class InMemoryBook(object):
    """ 

    Provide the same access to book data as a row from the BookTable dataset
    
    """

    def __getitem__(self, key):
        return self.__data[key]
    
    def __init__(self, book_record):
        self.__data = {
            'timestamp' : book_record['timestamp'],
            'timestamp_s' : book_record['timestamp_s'],
            'ask' : book_record['ask'].copy(),
            'bid' : book_record['bid'].copy(),
            'seqnum' : book_record['seqnum']
            }

    def move_bid_down(self, i):
        self.__data['bid'] = self.__data['bid'][i:]

    def reduce_top_bid_qty(self, qty):
        assert len(self.__data['bid']) > 0
        assert self.__data['bid'][0][1] > qty
        self.__data['bid'][0][1] -= qty

    def move_ask_up(self, i):
        self.__data['ask'] = self.__data['ask'][i:]

    def reduce_top_ask_qty(self, qty):
        assert len(self.__data['ask']) > 0
        assert self.__data['ask'][0][1] > qty
        self.__data['ask'][0][1] -= qty

    def advance_timestamp(self, timestamp, timestamp_s):
        assert timestamp >= self.__data['timestamp']
        self.__data['timestamp'] = timestamp
        self.__data['timestamp_s'] = timestamp_s
        

class Book(object):
    """
    Provides utility methods on top of the HDF5 book records.
    Can wrap records of the BookTable or the InMemoryBook
    """
    
    def __init__(self, record):
        self.__record = record

    def timestamp(self):
        return self.__record['timestamp']

    def timestamp_s(self):
        return self.__record['timestamp_s']

    def seqnum(self):
        return self.__record['seqnum']

    def top(self):
        return (self.__record['bid'][0], self.__record['ask'][0])

    def topPx(self):
        return (self.__record['bid'][0][0], self.__record['ask'][0][0])
    
    def mid(self): 
        t = self.topPx()
        return ((t[0] + t[1])/2.0)

    def topQty(self):
        return (self.__record['bid'][0][1], self.__record['ask'][0][1])

    def level(self, i):
        return (self.__record['bid'][i], self.__record['ask'][i])

    def trade_at_top(self, trade_px):
        if(self.__record['bid'][0][0] == trade_px):
            return 'B'
        elif (self.__record['ask'][0][0] == trade_px):
            return 'A'

    def trade_improves_top(self, trade_px):
        """
        The returns tuple of (side, price_improvement) if this trade improves
        the book. Book improvement is defined as a trade *at* or *better* than
        top of book. Even trades at top of book improve it since they reduce
        quantity.
        """
        bid_top_px, ask_top_px = self.__record['bid'][0][0], self.__record['ask'][0][0]
        if(bid_top_px >= trade_px):
            return ('B', bid_top_px - trade_px) 
        elif (ask_top_px <= trade_px):
            return ('A', trade_px - ask_top_px)
        else:
            return None
    
    def __str__(self):
        """
        Prints the book as ladder, asks then bids
        """
        result = [ str((self.__record['timestamp_s'], self.__record['seqnum'])) ]
        for ask in reversed(self.__record['ask']):
            result.append( str((ask[0], ask[1])) )

        for bid in self.__record['bid']:
            result.append( "\t"+str((bid[0], bid[1])) )
        return string.join(result, '\n')
