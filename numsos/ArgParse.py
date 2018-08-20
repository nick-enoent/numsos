from __future__ import print_function
import argparse
import datetime as dt
import time
import sys

def valid_date(date_str):
    if '/' in date_str:
        ccfmt_str = "%Y/%m/%d %H:%M"
        fmt_str = "%y/%m/%d %H:%M"
    else:
        ccfmt_str = "%Y-%m-%d %H:%M"
        fmt_str = "%y-%m-%d %H:%M"
    try:
        return dt.datetime.strptime(date_str, fmt_str)
    except ValueError:
        try:
            return dt.datetime.strptime(date_str, ccfmt_str)
        except:
            msg = "{0} is not a valid date".format(date_str)
            raise argparse.ArgumentTypeError(msg)

def fmt_begin_date(days):
    now = int(dt.datetime.now().strftime('%s'))
    now -= (days * 24 * 60 * 60)
    return dt.datetime.fromtimestamp(now)

class ArgParse(object):
    def __init__(self, description):
        self.parser = argparse.ArgumentParser(description=description)
        self.parser.add_argument(
            "--path", required=True,
            help="The path to the database.")
        self.parser.add_argument(
            "--create", action="store_true",
            help="Create a new SOS database. " \
            "The --path parameter specifies the path to the new " \
            "database.")
        self.parser.add_argument(
            "--mode", metavar="FILE-CREATION-MASK", type=int,
            help="The permissions to assign to SOS database files.")
        self.parser.add_argument(
            "--verbose", action="store_true",
            help="Request verbose query output")
        self.parser.add_argument(
            "--monthly", action="store_true",
            help="Show results in the last 30 days")
        self.parser.add_argument(
            "--weekly", action="store_true",
            help="Show results in the last 7 days")
        self.parser.add_argument(
            "--daily", action="store_true",
            help="Show results in the last 24 hours")
        self.parser.add_argument(
            "--today", action="store_true",
            help="Show today's results (since midnight)")
        self.parser.add_argument(
            "--begin",
            type=valid_date,
            help="Specify the start time/date for similar jobs. " \
            "Format is [CC]YY/MM/DD HH:MM or [CC]YY-MM-DD HH:MM")
        self.parser.add_argument(
            "--end",
            type=valid_date,
            help="Specify the end time/date for similar jobs. ")

    def add_argument(self, *args, **kwargs):
        return self.parser.add_argument(*args, **kwargs)

    def parse_args(self):
        args = self.parser.parse_args()
        if args.today or args.daily or args.weekly or args.monthly:
            if args.begin or args.end:
                print("--begin/end and the --daily/weekly/monthly options "
                      "are mutually exclusive")
                sys.exit(1)

        if args.today:
            now = dt.datetime.now()
            args.begin = dt.datetime(now.year, now.month, now.day)

        if args.daily:
            args.begin = fmt_begin_date(1)

        if args.weekly:
            args.begin = fmt_begin_date(7)

        if args.monthly:
            args.begin = fmt_begin_date(30)

        return args

    def get_times(self, args):
        if args.begin:
            start = int(args.begin.strftime("%s"))
        else:
            start = 0
        if args.end:
            end = int(args.end.strftime("%s"))
        else:
            end = 0
        return (start, end)

