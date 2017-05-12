
import os
import sys
import argparse
import yaml
import time
import simplejson as json
from urllib.request import urlopen
from urllib.error import HTTPError
from multiprocessing import Pool
from multiprocessing import Lock

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


class settings():
    API_KEY      = 'demo'
    PROC_NUM     = 1
    CHUNK_SIZE   = 90
    OUTPUT_DIR   = './tmp'


class StockDataDownloader(object):

    def __init__(self, symbols, mode):
        self._symbols      = symbols
        self._url_template = 'http://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&outputsize={1}&apikey={2}'
        self._db           = Database('test')
        self._mode         = mode


    def run(self, proc_num):
        with Pool(processes=proc_num) as pool:
            while self._symbols:
                symbols_chunk = self._chunk_data(self._symbols[:settings.CHUNK_SIZE], proc_num)

                start_time = time.time()

                pool.map(self._get_data, symbols_chunk)

                end_time = time.time()

                self._symbols = self._symbols[settings.CHUNK_SIZE:]

                duration = end_time - start_time

                print(duration)

                # to lighten up the load on API
                # sleep_time = 60 - duration

                # if symbols and sleep_time > 0:
                #     time.sleep(sleep_time)


    def _get_data(self, symbols_chunk):
        for symbol in symbols_chunk:
            url = self._url_template.format(symbol, OUTPUT_SIZE[self._mode], settings.API_KEY)

            try:
                with urlopen(url) as response:
                    data = json.load(response)

            except HTTPError as e:
                self._print_err(symbol, 'HTTP error {0}'.format(e.code))
                continue

            if DAILY not in data:
                self._print_err(symbol, 'no data')
                continue

            self._write(symbol, data[DAILY])


    def _print_err(self, symbol, msg):
        error_msg = '{0} - {1}'.format(symbol, msg)
        with LOCK:
            print(error_msg, file=sys.stderr)


    def _write(self, symbol, data):
        buff = []

        for date, info in data.items():
            date_split = date.split()

            if len(date_split) == 2:
                date = date_split[0]

            buff.append((date, info[OPEN], info[HIGH], info[LOW], info[CLOSE], info[VOLUME]))

        buff.sort()

        try:
            self._write_to_db(symbol, buff)

        except Exception as e:
            self._print_err(symbol, 'error occured while trying to write to db: {0}'.format(e))

        output_file_path = os.path.join(settings.OUTPUT_DIR, symbol)

        with open(output_file_path, 'w') as output_file:
            for row in buff:
                try:
                    self._write_to_file(output_file, row)

                except IOError as e:
                    self._print_err(symbol, 'error occured while trying to write to file')
                    break


    def _write_to_file(self, output_file, row):
        line = '{0} {1} {2} {3} {4} {5}\n'.format(*row)
        output_file.write(line)


    def _write_to_db(self, symbol, buff):
        self._db.write_rows(symbol, buff)


    def _chunk_data(self, data, num):
        avg = len(data) / float(num)
        out = []
        last = 0.0

        while last < len(data):
            out.append(data[int(last):int(last + avg)])
            last += avg

        return out


def load_settings(path):
    with open(path) as f:
        s = yaml.load(f)

    for key, value in s.items():
        setattr(settings, key.upper(), value)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--settings', dest='settings_path', metavar='PATH', default='', help='path to the settings file')
    parser.add_argument('input_file_path', metavar='PATH', help='input file path with stock symbols')
    parser.add_argument('mode', metavar='MODE', choices=[IMPORT, UPDATE], help='\'{0}\' or \'{1}\''.format(IMPORT, UPDATE))

    options = parser.parse_args()

    load_settings(options.settings_path)

    symbols = []

    with open(options.input_file_path) as input_file:
        for line in input_file:
            symbol = line.rstrip('\n').upper()
            symbols.append(symbol)

    sdd = StockDataDownloader(symbols, options.mode)
    sdd.run(settings.PROC_NUM)


if __name__ == '__main__':
    main()
