from tables import *
from numpy import *
from attribute import readable, writable
from auction.book import Book, BookTable

__FLUSH_FREQ__ = 10000
__LEVELS__ = 10
__TICK_SIZE__ = 10000

class PriceOrderedDict(object):
    """
    Dictionary with keys sorted by price - quite similar to effect of an
    std::map.
    """
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

class FileRecordCounter(object):
    """
    Periodically the file gets flushed. Flushing too frequently can hurt
    performance. Multiple datasets are stored in a single file, so the count
    is based on number of adds to *any* of the data sets.
    """
    readable(h5_file = None, count = 0)

    def __init__(self, h5_file):
        """
        H5 file object to flush
        """
        self.__h5_file = h5_file
        self.__count = 0

    def increment_count(self):
        """
        Increment counter and flush if __FLUSH_FREQ__ records have been added
        """
        self.__count += 1
        if 0 == (self.__count % __FLUSH_FREQ__):
            self.__h5_file.flush()

class BookBuilder(object):
    """
    Processes Add/Modify/Delete records to build books per symbol
    """

    readable(unchanged=0)

    _book_files_ = {}

    symbol = property(lambda self: self._symbol, None, None, 
                      r"Symbol for the book")

    def __init__(self, symbol, h5_file, **rest):
        self._file_record_counter = BookBuilder._book_files_.get(h5_file, None)
        if not self._file_record_counter:
            BookBuilder._book_files_[h5_file] = FileRecordCounter(h5_file)
            self._file_record_counter = BookBuilder._book_files_[h5_file]

        h5_file = self._file_record_counter.h5_file
        filters = Filters(complevel=1, complib='zlib')
        group = h5_file.createGroup("/", symbol, 'Book data')
        self._table = h5_file.createTable(group, 'books', BookTable, 
                                           "Data for "+str(symbol), filters=filters)
        self._tick_size = rest.get('tick_size', None) or __TICK_SIZE__ # TODO
        self._record = self._table.row
        self._symbol = symbol
        self._orders = {}
        self._bids_to_qty = PriceOrderedDict(False)
        self._asks_to_qty = PriceOrderedDict()
        self._bids = zeros(shape=[__LEVELS__,2])
        self._asks = zeros(shape=[__LEVELS__,2])
        self._unchanged = 0


    def summary(self):
        """
        Prints some summary information for a parse
        """
        print "Completed data for:", self._symbol
        print "\tOutstanding orders:", len(self._orders)
        print "\tOutstanding bids:", len(self._bids_to_qty)
        print "\tOutstanding asks:", len(self._asks_to_qty)
        print "\tUnchanged:", self._unchanged
        # Any left over data and this was not a success
        if len(self._orders) or len(self._bids_to_qty) or len(self._asks_to_qty):
            return False
        else:
            return True


    def make_record(self, ts, ts_s):
        """
        A new record has been processed and the bids and asks updated
        accordingly. This takes the new price data and updates the book and
        timestamps for storing.
        """
        previous_bids = self._bids.copy()
        previous_asks = self._asks.copy()

        bid_top = self._bids_to_qty.top()
        if bid_top:
            for i, px in enumerate(range(bid_top, 
                                         bid_top-__LEVELS__*self._tick_size, 
                                         -self._tick_size)):
                self._bids[i][0] = px
                self._bids[i][1] = self._bids_to_qty.get_quantity(px)
        else:
            self._bids = zeros(shape=[__LEVELS__,2])

        ask_top = self._asks_to_qty.top()
        if ask_top:
            for i, px in enumerate(range(ask_top, 
                                         ask_top+__LEVELS__*self._tick_size, 
                                         self._tick_size)):
                self._asks[i][0] = px
                self._asks[i][1] = self._asks_to_qty.get_quantity(px)
        else:
            self._asks = zeros(shape=[__LEVELS__,2])

        self._record['bid'] = self._bids
        self._record['ask'] = self._asks
        self._record['timestamp'] = ts
        self._record['timestamp_s'] = ts_s

        if bid_top and ask_top and bid_top >= ask_top:
            msg = [((bid_top==ask_top) and "Locked " or "Crossed "),
                   "Market %s"%self._symbol, 
                   str((bid_top, ask_top))]
            raise RuntimeError(string.join(msg,':'))        

        if (self._bids == previous_bids).all() and (self._asks == previous_asks).all():
            self._unchanged += 1
        else:
            self._record.append()
            self._file_record_counter.increment_count()
        

    def process_record(self, amd_record):
        raise RuntimeError("process_record Subclass Responsibility")
