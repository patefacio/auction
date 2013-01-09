###############################################################################
#
# File: cme_fix_parser.py
#
# Description: Parser for cme fix files
#
##############################################################################
from path import path
from attribute import readable, writable
from auction.paths import *
import os
import zipfile
import re

class CmeFixParser(object):
    r"""

Parses one or more fix files and creates an HDF5 book.
Acutally is more than a parser as it also makes assumptions about input file
naming conventions. 

"""

    readable(input_path=None, output_path=None) 

    match_all = re.compile(".*")

    def __init__(self, input_path, output_path, input_match_re = match_all):
        """
        input_path - path to folder containing files to be parsed and output
        output_path - path to folder containing output data
        """
        self.__input_path = input_path
        self.__output_path = output_path
        self.__input_match_re = input_match_re
        self.match_inputs()

    def match_inputs(self):
        if not self.__input_path.exists():
            raise RuntimeError("Input path does not exist " + self.__input_path)
        i = 0
        for line in open(self.input_path):
            if i < 10:
                print line
                i += 1

            

if __name__ == "__main__":
    import pprint
    here = path(os.path.realpath(__file__))
    parser = CmeFixParser(here.parent.parent.parent.parent / 'data' / 'uncompressed' / 'XCME_ES_ES_FUT_20120215.txt',2)
    pprint.pprint(vars(parser))

