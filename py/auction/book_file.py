from path import path
from attribute import readable, writable

class BookFileReader(object):
    r"""
A handle on an hdf file with standardized book data to read
"""

    readable(file_path=None) 

    def __init__(self, file_path):
        """

        """
        self.__file_path = file_path

class BookFileWriter(object):
    r"""
A handle on an hdf file with standardized book data to write
"""

    readable(file_path=None) 

    def __init__(self, file_path):
        self.__file_path = file_path

        

if __name__ == "__main__":
    import os
    import pprint
    here = path(os.path.realpath(__file__))
    writer = BookFileWriter(here.parent.parent.parent / 'data' / 'book_data' / 'ES.h5')
    pprint.pprint(vars(writer));
    print path(writer.file_path.parent.exists())
    
