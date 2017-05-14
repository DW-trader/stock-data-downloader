
import sys
from os import listdir
from os.path import isfile, join
import argparse

from database import Database


def import_stock_data(db, symbol, input_file):
    rows = []

    for line in input_file:
        line = line.rstrip('\n')
        rows.append(line.rsplit(' ', 5))

    db.write_rows('daily_stock_data', symbol, rows)
    print(symbol)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('input_directory_path', metavar='PATH', help='directory path with stock data')

    options = parser.parse_args()

    symbols = [f for f in listdir(options.input_directory_path) if isfile(join(options.input_directory_path, f))]

    db = Database('stock_data')

    for symbol in symbols:
        with open(join(options.input_directory_path, symbol)) as input_file:
            try:
                import_stock_data(db, symbol, input_file)
            except Exception as e:
                print('{0} - {1}'.format(symbol, e), file=sys.stderr)


if __name__ == '__main__':
    main()
