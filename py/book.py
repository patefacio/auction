import tables
import time
from tables import *
from random import random
from numpy import array
from scipy.stats import describe

class BookTable(IsDescription):
    timestamp   = Int64Col()
    symbol      = StringCol(8) 
    ask         = Int64Col(shape=(5,2))
    bid         = Int64Col(shape=(5,2))    

class Book(object):
    
    def __init__(self, record):
        self.__record = record

    def symbol(self):
        return self.__record['symbol']

    def timestamp(self):
        return self.__record['timestamp']

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


filename = "book.h5"
h5file = openFile(filename, mode = "w", title = "My es book data for 2001/1/1")
group = h5file.createGroup("/", 'ES', 'Market book data')
table = h5file.createTable(group, 'es_book', BookTable, "2001/1/1")
book = table.row

current = 100
for i in xrange(10):
    book['timestamp'] = int(time.time()*10e6)
    book['symbol']  = 'ES'
    book['bid'] = array([ 
        [ current, 2*i ],
        [ current-1, 2*i ],
        [ current-2, 2*i ],
        [ current-3, 2*i ],
        [ current-4, 2*i ],
        ])

    book['ask'] = array([ 
        [ current+1, 2*i ],
        [ current+2, 2*i ],
        [ current+3, 2*i ],
        [ current+4, 2*i ],
        [ current+5, 2*i ],
        ])

    current = current + ((random()<0.5) and int(random()*10) or -int(random()*10))
    book.append()

h5file.close()

################################################################################
# Demonstrate opening h5 file and iterating on the book, printing top of book
################################################################################
f=tables.openFile(filename)
mid_px = []
for record in f.root.ES.es_book[:]:
    b = Book(record)
    mid_px.append(b.mid())
    print "TOP: time(%d) %s Px:" % (b.timestamp(), b.symbol()), \
        b.topPx(),"\tmid:", b.mid(), "\tQty: ", b.topQty()
    
print describe(array(mid_px))
