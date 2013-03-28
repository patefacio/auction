from auction.time_utils import *
from auction.paths import *
from auction.parser.parser_summary import ParseManager
from path import path
import logging
import os
from auction.parser.cme_date_utils import CmeRawFileSet

if __name__ == "__main__":
    import pprint
    from prettytable import PrettyTable
    fileset = CmeRawFileSet()
    outfile_map = {}
    outfiles = CME_OUT_PATH.files()
    table = PrettyTable(["Date", "Type", "Status", "Start", "End", "Warnings"])
    table.padding_width = 1 # One space between column edges and contents (default)
    table.sortby = "Date"
    table.align["Date"] = "l"
    for f in outfiles:
        d = get_date_of_file(f)
        if not d:
            print "Warning: found cme output file with no date", f
            continue
        dup = outfile_map.get(d)
        if dup:
            print "Warning: dup for date", d
        outfile_map[d] = f
        record = fileset.date_map.get(d)
        if not record:
            print "Could not find record for date", d
        else:
            try:
                summary = ParseManager.get_summary_record(f)
                datestr = str(d) + d.strftime(" (%A)")
                if summary:
                    table.add_row([datestr, record['type'], "Valid" if summary['is_valid'] else "Invalid",
                                   chicago_time(summary['data_start']), 
                                   chicago_time(summary['data_stop']),
                                   '?'])
                else:
                    table.add_row([datestr, record['type'], "No Summary", "", "", ""])
            except Exception, e:
                print "Caught exception:", e
                table.add_row([d, record['type'], "No Summary", "", "", ""])
    print table
    print "Num rows:", table.rowcount
