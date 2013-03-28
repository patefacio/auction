import os
from path import path
__HERE__ = path(os.path.realpath(__file__))

AUCTION_PATH = __HERE__.parent.parent.parent

# Root for data files
DATA_PATH = AUCTION_PATH / 'data'

# Location of compressed files
COMPRESSED_DATA_PATH = DATA_PATH / 'compressed'

# Location for uncompressed files
UNCOMPRESSED_DATA_PATH = DATA_PATH / 'uncompressed'

# Location for h5 book data
BOOK_DATA = AUCTION_PATH / 'book_data'

CME_OUT_PATH = BOOK_DATA / 'cme'
ARCA_OUT_PATH = BOOK_DATA / 'arca'
