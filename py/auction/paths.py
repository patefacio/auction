import os
from path import path
__HERE__ = path(os.path.realpath(__file__))
DATA_PATH = __HERE__.parent.parent.parent / 'data'
COMPRESSED_DATA_PATH = DATA_PATH / 'compressed'
UNCOMPRESSED_DATA_PATH = DATA_PATH / 'uncompressed'
BOOK_DATA = DATA_PATH / 'book_data'
