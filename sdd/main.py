
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


def get_symbols():
    request = urlopen('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt')

    symbols = []

    for line in request:
        line = line.decode('UTF-8')
        symbol, *_ = line.split('|')

        if symbol.isupper():
            symbols.append(symbol)
            break

    return symbols


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--settings', dest='settings_path', metavar='PATH', default='', help='path to the settings file')
    parser.add_argument('mode', metavar='MODE', choices=[IMPORT, UPDATE], help='\'{0}\' or \'{1}\''.format(IMPORT, UPDATE))

    options = parser.parse_args()

    load_settings(options.settings_path)

    downloader = Downloader(get_symbols(), options.mode)
    downloader.run()


if __name__ == '__main__':
    main()
