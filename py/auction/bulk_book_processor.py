from auction.time_utils import *
from auction.paths import *
from multiprocessing import Process, Pool
from path import path
import subprocess
import logging
import os
import re

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    import pprint
    __HERE__ = path(os.path.realpath(__file__))

    def generate_book_data(input):
        args = ["python", __HERE__.parent / "book_processor.py",] + [ '-d', get_date_string(get_date_of_file(input)) ]
        logging.info("Generating data input: %s =>\n\t%s"%(input.name, args))
        subprocess.call(args)

    p = Pool(22)
    dateRe = re.compile(r'\d\d\d\d\d\d\d\d$')
    input_files = filter(lambda f: dateRe.search(f), CME_OUT_PATH.files())
    p.map(generate_book_data, input_files)

