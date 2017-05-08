
import sys


input_file_path = sys.argv[1]


with open(input_file_path) as input_file:
    next(input_file)

    for line in input_file:
        info = line.rstrip('\n').split(',')

        print info[0][1:-1]
