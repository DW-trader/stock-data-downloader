
import os
import sys
import time
import datetime
import calendar
import pytz
import simplejson as json
from urllib.request import urlopen
from urllib.error import HTTPError
from multiprocessing import Pool
from multiprocessing import Lock

from settings import Settings
from database import Database


OPEN   = '1. open'
HIGH   = '2. high'
LOW    = '3. low'
CLOSE  = '4. close'
VOLUME = '5. volume'

IMPORT = 'import'
UPDATE = 'update'

OUTPUT_SIZE = {
    IMPORT : 'full',
    UPDATE : 'compact'
}

DAILY = 'Time Series (Daily)'

LOCK = Lock()

DB = None


class Downloader(object):

    def __init__(self, symbols, mode):
        self._symbols        = symbols
        self._url_template   = 'http://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&outputsize={1}&apikey={2}'
        self._mode           = mode


    def run(self):
        def _init_db(db_name):
            global DB
            DB = Database(db_name)

        proc_num = Settings.PROC_NUM

        with Pool(processes=proc_num, initializer=_init_db, initargs=('stock_data',)) as pool:
            while self._symbols:
                symbols_chunk = self._chunk_data(self._symbols[:Settings.CHUNK_SIZE], proc_num)

                start_time = time.time()

                pool.map(self._get_data, symbols_chunk)

                end_time = time.time()

                self._symbols = self._symbols[Settings.CHUNK_SIZE:]

                duration = end_time - start_time

                # to lighten up the load on API
                sleep_time = 60 - duration

                if self._symbols and sleep_time > 0:
                    print('sleeping {0} seconds'.format(sleep_time))
                    time.sleep(sleep_time)

                print('{0} symbols left'.format(len(self._symbols)))


    def _get_data(self, symbols_chunk):
        for symbol in symbols_chunk:
            url = self._url_template.format(symbol, OUTPUT_SIZE[self._mode], Settings.API_KEY)

            try:
                with urlopen(url) as response:
                    data = json.load(response)

            except HTTPError as e:
                self._print_err(symbol, 'HTTP error {0}'.format(e.code))
                continue

            if DAILY not in data:
                self._print_err(symbol, 'no data')
                continue

            write_data = self._get_write_data(symbol, data[DAILY])
            self._write(symbol, write_data)


    def _get_write_data(self, symbol, data):
        buff = []

        if self._mode == UPDATE:
            last_timestamp = DB.get_last_timestamp(symbol)

        for date, info in data.items():
            timestamp = self._date_to_ny_utc(date)

            if self._mode == IMPORT or timestamp > last_timestamp:
                buff.append((timestamp, info[OPEN], info[HIGH], info[LOW], info[CLOSE], info[VOLUME]))

        buff.sort()

        return buff


    def _print_err(self, symbol, msg):
        error_msg = '{0} - {1}'.format(symbol, msg)
        with LOCK:
            print(error_msg, file=sys.stderr)


    def _write(self, symbol, data):
        try:
            self._write_to_db(symbol, data)
        except Exception as e:
            self._print_err(symbol, 'error occured while trying to write to db: {0}'.format(e))

        try:
            self._write_to_file(symbol, data)
        except:
            self._print_err(symbol, 'error occured while trying to write to file')


    def _write_to_file(self, symbol, buff):
        output_dir = os.path.join(Settings.OUTPUT_DIR, symbol[0])
        output_file_path = os.path.join(output_dir, symbol)

        if self._mode == IMPORT or not os.path.exists(output_file_path):
            write_mode = 'w'
        else:
            write_mode = 'a'

        with open(output_file_path, write_mode) as output_file:
            for row in buff:
                line = '{0} {1} {2} {3} {4} {5}\n'.format(*row)
                output_file.write(line)


    def _write_to_db(self, symbol, buff):
        DB.write_rows('daily_stock_data', symbol, buff)


    def _chunk_data(self, data, num):
        avg = len(data) / float(num)
        out = []
        last = 0.0

        while last < len(data):
            out.append(data[int(last):int(last + avg)])
            last += avg

        return out


    def _date_to_ny_utc(self, date):
        date_split = date.split()

        if len(date_split) == 2:
            date = date_split[0]

        date = '{0} 16:00:00'.format(date)
        ny_local = pytz.timezone('America/New_York')
        naive = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        ny_local_dt = ny_local.localize(naive, is_dst=None)

        return calendar.timegm(ny_local_dt.astimezone(pytz.utc).timetuple())
