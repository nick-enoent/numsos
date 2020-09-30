from __future__ import print_function
from builtins import object
import argparse
import datetime as dt
import re
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

def period_spec(period):
    s = "(?P<count>[0-9]+)(?P<units>[smhd])"
    x = re.compile(s)
    m = re.match(x, period)
    if m is None:
        msg = "{0} is not a valid period specification".format(period)
        raise argparse.ArgumentTypeError(msg)
    count = m.group(1)
    units = m.group(2)
    if units == 's':
        return int(count)
    elif units == 'm':
        return int(count) * 60
    elif units == 'h':
        return int(count) * 3600
    elif units == 'd':
        return int(count) * 86400
    msg = "{0} {1} {2} is not a valid period specification".format(period, count, units)
    raise argparse.ArgumentTypeError(msg)

def fmt_begin_date(days):
    now = int(dt.datetime.now().strftime('%s'))
    now -= (days * 24 * 60 * 60)
    return dt.datetime.fromtimestamp(now)

class ArgParse(object):
    """A convenience class for building configuring argparse for analysis commands.

    The class provides arguments for entering date and time
    constraints from the command line, for example 'hourly', 'daily',
    'weekly', 'monthly', 'begin', and 'end'. Options for accepting
    container location and creation options are also provided.

    After calling parse_args, the following class attributes are
    populated:

    begin - A datetime object specifying the start of the analysis
            period.
    end   - A datetime object specifying the end of the analysis period.

    -- Postional Parameters

        - A description for the command that is displayed in the usage message

    -- Keyword Parameters

    options= If None, all options are included, otherwise:
        'datetime' - Include all the date convenience options
        'create'   - Include the container create mode options.
    """
    def __init__(self, description, options=None):
        self.begin = self.end = None
        self.parser = argparse.ArgumentParser(description=description)
        self.parser.add_argument(
            "--path", required=True,
            help="The path to the database.")
        self.parser.add_argument(
            "--verbose", action="store_true",
            help="Request verbose query output")
        if options is None or 'create' in options:
            self.parser.add_argument(
                "--create", action="store_true",
                help="Create a new SOS database. " \
                "The --path parameter specifies the path to the new " \
                "database.")
            self.parser.add_argument(
                "--mode", metavar="FILE-CREATION-MASK", type=int,
                help="The permissions to assign to SOS database files.")
        if options is None or 'datetime' in options:
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
                "--hourly", action="store_true",
                help="Show results in the last hour")
            self.parser.add_argument(
                "--begin",
                type=valid_date,
                help="Specify the start time/date for similar jobs. " \
                "Format is [CC]YY/MM/DD HH:MM or [CC]YY-MM-DD HH:MM")
            self.parser.add_argument(
                "--end",
                type=valid_date,
                help="Specify the end time/date for similar jobs. ")
            self.parser.add_argument(
                "--period",
                type=period_spec,
                help="Specify a period for the analysis." \
                "The format is [count][units] where," \
                "  count : A number\n" \
                "  units :\n" \
                "        s - seconds\n" \
                "        m - minutes\n" \
                "        h - hours\n" \
                "        d - days\n")

    def add_argument(self, *args, **kwargs):
        """Add an additional option to the argument parser.

        NB: While it's not clear to the author why this isn't called
        add_option, this class minics the underlying Python
        argparser's naming
        """
        return self.parser.add_argument(*args, **kwargs)

    def parse_args(self):
        """Parse the command line options"""
        args = self.parser.parse_args()
        if args.today or args.daily or args.weekly or args.monthly:
            if args.begin or args.end:
                print("--begin/end and the --daily/weekly/monthly options "
                      "are mutually exclusive")
                sys.exit(1)

        if args.today:
            now = dt.datetime.now()
            args.begin = dt.datetime(now.year, now.month, now.day)

        if args.hourly:
            args.begin = fmt_begin_date(1.0 / 24.0)

        if args.daily:
            args.begin = fmt_begin_date(1)

        if args.weekly:
            args.begin = fmt_begin_date(7)

        if args.monthly:
            args.begin = fmt_begin_date(30)

        return args
