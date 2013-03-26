from path import path
from attribute import readable, writable
from tables import *
from auction.book import Book, InMemoryBook, BookTable
from auction.paths import *
import sys
import re

class H5Repository(object):
    """ 
    Keeps a map of path_to_file to hdf5 file object. 
    
    For ARCA there could be multiple symbols in one file and you only want one
    hdf file object per real file.
    """

    _repository = {}

    @staticmethod
    def get_opened_file(path_to_file):
        result = H5Repository._repository.get(path_to_file)
        if not result:
            result = openFile(path_to_file)
            H5Repository._repository[path_to_file] = result
        return result    
        

    @staticmethod
    def find_data_file(date, symbol):

        if re.compile(r'\w+(?:[HMUZ])\d').match(symbol):
            result = CME_OUT_PATH / date
        else:
            result = ARCA_OUT_PATH / date

        if not result.exists():
            raise Exception("Could not find data file for (%s, %s) at %s"%(date,symbol,result))

        return H5Repository.get_opened_file(result)


class BookStream(object):
    """
    This is the basic stream which can be used for book iteration on any
    dataset storing rows of the BookTable format. To allow for iteration it
    provides the required methods:
      __iter__: Just returns the stream object
      next: returs the next book object in the stream

    So an example usage:

    reader = BookStream('20111017', 'ESZ1')
    for record in reader:
       book = Book(record)
       ...

    """
    readable(date=None, symbol=None, input_file=None, book=None, book_count=None, book_ds = None)

    def __init__(self, date, symbol):
        self.__date = date
        self.__symbol = symbol
        self.__input_file = H5Repository.find_data_file(date, symbol)
        self.__index = 0
        self.__book_ds = filter(lambda n: n._v_name == symbol, self.__input_file.root)[0].books
        self.__book_count = self.__book_ds.nrows
        self._current_book = None

    def __iter__(self):
        return self

    def peek_book(self):
        if self.__index < self.__book_count:
            return self.__book_ds[self.__index]
        return None

    def next(self):
        if self.__index < self.__book_count:
            self._current_book = self.__book_ds[self.__index]
            self.__index += 1
            return self._current_book
        else:
            print "Stopping Iteration:", self.__index, "vs", self.__book_count
            raise StopIteration

class CmeImpliedBookStream(BookStream):
    """
    This extends the BookStream iteration to support CME trade throughs.  The
    class tracks both books and trades. If the timestamp of the next trade is
    before the timestamp of the next book, it implies a book from the current
    book by pulling out the trade quantities.

    One aspect of the way this works with iteration is, if the the trade does
    not improve the book, where improve means trade at the top price or
    better, then the current book is returned *again*.

    """
    def __init__(self, date, symbol):
        BookStream.__init__(self, date, symbol)
        self.__trade_ds = filter(lambda n: n._v_name == symbol, self.input_file.root)[0].trades
        self.__trade_count = self.__trade_ds.nrows
        self.__trade_index = 0
        self.__trade = None
        if self.__trade_count > 0:
            self.__next_trade = self.__trade_ds[self.__trade_index]
        else:
            self.__next_trade = None
        self.__implied_book = None
        self.__logged_original = False

    def peek_trade(self):
        if self.__trade_index < self.__trade_count:
            return self.__trade_ds[self.__trade_index]
        return None

    def next(self):
        next_trade = self.peek_trade()
        next_book = self.peek_book()
        current_book = self._current_book

        # If next trade timestamp is ahead of book timestamp, imply a book
        if next_trade and next_book and next_trade['seqnum'] <= next_book['seqnum']:
            self.__trade_index += 1
            trade_price = next_trade['price']
            book_object = Book(current_book)

            # See if this trade_price improves the book and if so make/use implied book
            improvement = book_object.trade_improves_top(trade_price)
            if improvement:
                
                side = improvement[0]
                improve_amount = improvement[1]
                trade_qty = next_trade['quantity']

                # Use timestamp/seqnum of trade to push InMemoryBook forward
                timestamp = next_trade['timestamp']
                timestamp_s = next_trade['timestamp_s']
                seqnum = next_trade['seqnum']

                # Track how much of the book has been eaten at each price
                if not self.__implied_book:
                    self.__implied_book = InMemoryBook(current_book)

                if improve_amount > 0:
                    if not self.__logged_original:
                        print "------------------------------------------------------"
                        print "Original Book", Book(self._current_book)
                        self.__logged_original = True

                    print "Trade %s (%s @ %s) improves %s"%(seqnum, next_trade[1], next_trade[0], improvement)
                    
                # iterate over the implied book and draw down existing quantities
                if side == 'B':
                    for i, bid in enumerate(self.__implied_book['bid']):
                        if bid[0] == trade_price:
                            if bid[1] > trade_qty:
                                self.__implied_book.move_bid_down(i)
                                self.__implied_book.reduce_top_bid_qty(trade_qty)
                            else:
                                self.__implied_book.move_bid_down(i+1)
                            self.__implied_book.advance_timestamp(timestamp, timestamp_s, seqnum)
                            break
                        elif bid[0] < trade_price:
                            self.__implied_book.move_bid_down(i)
                            self.__implied_book.advance_timestamp(timestamp, timestamp_s, seqnum)
                            break
                    if improve_amount > 0:
                        print "Bid Improved Implied\n", Book(self.__implied_book)
                else:
                    assert side == 'A'
                    for i, ask in enumerate(self.__implied_book['ask']):
                        if ask[0] == trade_price:
                            if ask[1] > trade_qty:
                                self.__implied_book.move_ask_up(i)
                                self.__implied_book.reduce_top_ask_qty(trade_qty)
                            else:
                                self.__implied_book.move_ask_up(i+1)
                            self.__implied_book.advance_timestamp(timestamp, timestamp_s, seqnum)
                            break
                        elif ask[0] > trade_price:
                            self.__implied_book.move_ask_up(i)
                            self.__implied_book.advance_timestamp(timestamp, timestamp_s, seqnum)
                            break
                    if improve_amount > 0:
                        print "Ask Improved Implied\n", Book(self.__implied_book)

                return self.__implied_book
            else:
                # Recurse to advance to next
                return self.next()
        else:
            # As soon as we get here we have book update, clear any implied data
            result = BookStream.next(self)
            if self.__implied_book:
                self.__implied_book = None
                if self.__logged_original:
                    self.__logged_original = False
                    book = Book(result)
                    print "Next Book After Trade Through", book 
            return result

if __name__ == "__main__":

    # Set up a book stream that deals with implied books
    # Note: if you did not want implied books you could just have
    # reader = BookStream('20111017', 'ESZ1')
    reader = CmeImpliedBookStream('20111017', 'ESZ1')

    # This iterates over all implied book records, so there will be a record
    # for each trade that improves as well as one for each book
    for b in reader:
        pass
