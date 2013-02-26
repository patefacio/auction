from auction.time_utils import *
from path import path
from auction.paths import *
from stat import *
from attribute import readable
from auction.time_utils import *
from multiprocessing import Process, Pool
import subprocess
import logging

__CME_SRC_PATH__ = DATA_PATH / 'CME_GLOBEX2'

__FFIX_RE__ = re.compile('FFIX')
__RLC1_RE__ = re.compile('RLC1')
__RLC2_RE__ = re.compile('RLC2')

class CmeRawFileSet(object):
    readable(file_map=None, date_map=None, ordered_dates=None, start_dates=None)

    def __init__(self):
        self.__file_map = {}
        self.__date_map = {}
        self.__ordered_dates = {}
        self.__start_dates = {}

        for f in __CME_SRC_PATH__.files():
            ftype = None
            if __FFIX_RE__.search(f):
                ftype = 'FFIX'
            elif __RLC1_RE__.search(f):
                ftype = 'RLC1'
            elif __RLC2_RE__.search(f):
                ftype = 'RLC2'
            else:
                continue

            date = get_date_of_file(f)
            record = { 'fname':f, 
                       'date':date,
                       'type': ftype,
                       'fstat':os.stat(f),
                       }

            assert None == self.__file_map.get(f.name)
            oops = self.__date_map.get(date)
            if oops:
                # Here there are multiple files for same date, select one
                if oops['type']=='RLC1' and record['type']=='RLC2':
                    logging.info("For date %s selecting RLC2 file"%str(date), record['fname'])
                elif oops['type']=='RLC2' and record['type']=='RLC1':
                    logging.info("For date %s selecting RLC2 file"%str(date), oops['fname'])
                    continue
                else:
                    assert False, "Two files for same date: %s\nand\n%s"%(str(oops), str(record))

            self.__file_map[f.name] = record
            self.__date_map[date] = record

        self.__ordered_dates = self.__date_map.keys()
        self.__ordered_dates.sort()
        for d in self.__ordered_dates:
            self.__start_dates.setdefault(get_previous_weekday(d, Sunday), []).append(d)

    def get_files(self, date_list):
        """Given list of dates, returns the file for each"""
        result = []
        for d in date_list:
            d = get_date_of_file(d)
            record = self.__date_map.get(d)
            assert record, "No record for date %s:"%str(d)
            result.append(record['fname'])
        return result

if __name__ == "__main__":
    import pprint
    fileset = CmeRawFileSet()
    __HERE__ = path(os.path.realpath(__file__))
    print "There are ", len(fileset.start_dates), "weeks"
