from auction.time_utils import *
from auction.paths import *
from multiprocessing import Process, Pool
from path import path
import subprocess
import logging
import os

__ARCA_SRC_PATH__ = DATA_PATH / 'NYSE_ARCA2'

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    import pprint
    __HERE__ = path(os.path.realpath(__file__))

    def generate_book_data(input):
        args = ["python", __HERE__.parent / "arca_parser.py",] + [ '-d', get_date_string(get_date_of_file(input)) ]
        logging.info("Generating data input: %s =>\n\t%s"%(input.name, args))
        subprocess.call(args)

    p = Pool(3)
    input_files = __ARCA_SRC_PATH__.files()
    p.map(generate_book_data, input_files)

