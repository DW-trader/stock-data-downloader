
import sys
import argparse
import yaml
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
    API_KEY = 'demo'


def get_data(tickers):
    for ticker in tickers:
        url_template = 'http://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&apikey={1}'
        url = url_template.format(ticker, settings.API_KEY)

        raw_data = urlopen(url)
        data = json.load(raw_data)

        if DAILY not in data:
            print('No data for {0}'.format(ticker), file=sys.stderr)
            continue

        for date, info in data[DAILY].items():
            print(ticker, date, info[OPEN], info[HIGH], info[LOW], info[CLOSE], info[VOLUME])


def _chunk_data(data, num):
    avg = len(data) / float(num)
    out = []
    last = 0.0

    while last < len(data):
        out.append(data[int(last):int(last + avg)])
        last += avg

    return out


def _load_settings(path):
    with open(path) as f:
        s = yaml.load(f)

    for key, value in s.items():
        setattr(settings, key.upper(), value)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--settings', dest='settings_path', metavar='PATH', default='', help='path to the settings file')
    parser.add_argument('-p', '--proc_num', dest='proc_num', type=int, metavar='NUM', default=1, help='number of processes')
    parser.add_argument('input_file_path', metavar='PATH', help='name of the test to run')

    options = parser.parse_args()

    _load_settings(options.settings_path)

    tickers = []

    with open(options.input_file_path) as input_file:
        for line in input_file:
            ticker = line.rstrip('\n').upper()
            tickers.append(ticker)

    proc_num = options.proc_num
    tickers = _chunk_data(tickers, proc_num)

    with Pool(processes=proc_num) as pool:
        pool.map(get_data, tickers)


if __name__ == '__main__':
    main()
