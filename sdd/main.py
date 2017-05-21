
import argparse
import yaml
from urllib.request import urlopen

from settings import Settings
from downloader import Downloader
from downloader import IMPORT, UPDATE


def load_settings(path):
    with open(path) as f:
        s = yaml.load(f)

    for key, value in s.items():
        setattr(Settings, key.upper(), value)


def get_symbols(ignore_path):
    ignore_list = []

    with open(ignore_path) as ignore_file:
        for line in ignore_file:
            ignore_symbol = line.rstrip('\n')
            ignore_list.append(ignore_symbol)

    request = urlopen('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt')

    symbols = []

    for line in request:
        line = line.decode('UTF-8')
        symbol, *_ = line.split('|')

        if symbol.isupper() and symbol not in ignore_list:
            symbols.append(symbol)

    return symbols


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--settings', dest='settings_path', metavar='PATH', default='', help='path to the settings file')
    parser.add_argument('-i', '--ignore', dest='ignore_path', metavar='PATH', default='', help='path to the file that has a list od symbols that should be ignored')
    parser.add_argument('mode', metavar='MODE', choices=[IMPORT, UPDATE], help='\'{0}\' or \'{1}\''.format(IMPORT, UPDATE))

    options = parser.parse_args()

    load_settings(options.settings_path)

    symbols = get_symbols(options.ignore_path)

    downloader = Downloader(symbols, options.mode)
    downloader.run()


if __name__ == '__main__':
    main()
