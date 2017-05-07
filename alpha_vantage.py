
import sys
import argparse
import yaml
import time
import simplejson as json
from urllib.request import urlopen
from multiprocessing import Pool


OPEN   = '1. open'
HIGH   = '2. high'
LOW    = '3. low'
CLOSE  = '4. close'
VOLUME = '5. volume'

DAILY = 'Time Series (Daily)'


class settings():
    API_KEY     = 'demo'
    PROC_NUM    = 1
    CHUNK_SIZE  = 90
    OUTPUT_SIZE = 'full'


def _get_data(symbols):
    for symbol in symbols:
        url_template = 'http://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&outputsize={1}&apikey={2}'
        url = url_template.format(symbol, settings.OUTPUT_SIZE, settings.API_KEY)

        raw_data = urlopen(url)
        data = json.load(raw_data)

        if DAILY not in data:
            print('No data for {0}'.format(symbol), file=sys.stderr)
            continue

        file_name = './tmp/{0}'.format(symbol)

        with open(file_name, 'w') as output_file:
            _write_to_file(output_file, data[DAILY])


def _write_to_file(output_file, data):
    buff = []
    for date, info in data.items():
        line = '{0} {1} {2} {3} {4} {5}\n'.format(date, info[OPEN], info[HIGH], info[LOW], info[CLOSE], info[VOLUME])
        buff.append(line)

    buff.sort()
    for line in buff:
        output_file.write(line)


def _chunk_data(data, num):
    avg = len(data) / float(num)
    out = []
    last = 0.0

    while last < len(data):
        out.append(data[int(last):int(last + avg)])
        last += avg

    return out


def run(symbols, proc_num):
    while symbols:
        symbols_chunk = symbols[:settings.CHUNK_SIZE]
        symbols_chunk = _chunk_data(symbols_chunk, proc_num)

        start_time = time.time()

        with Pool(processes=proc_num) as pool:
            pool.map(_get_data, symbols_chunk)

        end_time = time.time()

        symbols = symbols[settings.CHUNK_SIZE:]

        duration = end_time - start_time

        print(duration)

        # to lighten up the load on API
        # sleep_time = 60 - duration

        # if symbols and sleep_time > 0:
        #     time.sleep(sleep_time)


def load_settings(path):
    with open(path) as f:
        s = yaml.load(f)

    for key, value in s.items():
        setattr(settings, key.upper(), value)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--settings', dest='settings_path', metavar='PATH', default='', help='path to the settings file')
    parser.add_argument('input_file_path', metavar='PATH', help='name of the test to run')

    options = parser.parse_args()

    load_settings(options.settings_path)

    symbols = []

    with open(options.input_file_path) as input_file:
        for line in input_file:
            symbol = line.rstrip('\n').upper()
            symbols.append(symbol)

    run(symbols, settings.PROC_NUM)


if __name__ == '__main__':
    main()
