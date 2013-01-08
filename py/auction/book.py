from tables import *

class BookTable(IsDescription):
    timestamp   = Int64Col()
    timestamp_s = StringCol(12)
    ask         = Int64Col(shape=(5,2))
    bid         = Int64Col(shape=(5,2))    

class Book(object):
    
    def __init__(self, record):
        self.__record = record

    def timestamp(self):
        return self.__record['timestamp']

    def timestamp_s(self):
        return self.__record['timestamp_s']

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
