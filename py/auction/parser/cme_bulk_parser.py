from auction.time_utils import *
from auction.parser.cme_date_utils import CmeRawFileSet
from multiprocessing import Process, Pool
from path import path
import subprocess
import logging
import os

if __name__ == "__main__":
    import pprint
    fileset = CmeRawFileSet()
    __HERE__ = path(os.path.realpath(__file__))

    def generate_book_data(d):
        record = fileset.date_map.get(d)
        if not record:
            print "NO SUNDAY DATA FOR", d
            return
        ftype = fileset.date_map[d]['type']
        week_dates = fileset.start_dates[d]
        args = [ "-d" ]
        for wd in week_dates:
            args.append(get_date_string(wd))
        logging.info("Generating data for week of", d, "=>", fileset.start_dates[d])
        sunday = get_date_string(d)

        if ftype == 'FFIX':
            subprocess.call(["python", __HERE__.parent / "cme_fix_parser.py",] + args)
        else:
            subprocess.call(["python", __HERE__.parent / "cme_rlc_parser.py",] + args)

    #pprint.pprint(fileset.start_dates)
    p = Pool(24)
    p.map(generate_book_data, fileset.start_dates.keys())
    print "There are ", len(fileset.start_dates), "weeks"
