
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


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('input_directory_path', metavar='PATH', help='directory path with stock data')

    options = parser.parse_args()

    symbols = [f for f in listdir(options.input_directory_path) if isfile(join(options.input_directory_path, f))]

    db = Database('test')

    for symbol in symbols:
        with open(join(options.input_directory_path, symbol)) as input_file:
            import_stock_data(db, symbol, input_file)


if __name__ == '__main__':
    main()
