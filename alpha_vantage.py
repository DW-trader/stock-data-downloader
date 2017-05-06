
import sys
import argparse
from urllib.request import urlopen
import yaml
import simplejson as json


OPEN   = '1. open'
HIGH   = '2. high'
LOW    = '3. low'
CLOSE  = '4. close'
VOLUME = '5. volume'

DAILY = 'Time Series (Daily)'


class settings():
    API_KEY = 'demo'


def get_data(input_file):
    for line in input_file:
        ticker = line.rstrip('\n').upper()

        url_template = 'http://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&apikey={1}'
        url = url_template.format(ticker, settings.API_KEY)

        raw_data = urlopen(url)
        data = json.load(raw_data)

        if DAILY not in data:
            print >> sys.stderr, 'No data for {0}'.format(ticker)
            continue

        for date, info in data[DAILY].items():
            print (ticker, date, info[OPEN], info[HIGH], info[LOW], info[CLOSE], info[VOLUME])


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

    with open(options.input_file_path) as input_file:
        get_data(input_file)


if __name__ == '__main__':
    main()
