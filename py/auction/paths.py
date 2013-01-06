import os
from path import path
__HERE__ = path(os.path.realpath(__file__))

# Root for data files
DATA_PATH = __HERE__.parent.parent.parent / 'data'

# Location of compressed files
COMPRESSED_DATA_PATH = DATA_PATH / 'compressed'

# Location for uncompressed files
UNCOMPRESSED_DATA_PATH = DATA_PATH / 'uncompressed'

# Location for h5 book data
BOOK_DATA = DATA_PATH / 'book_data'
