#!/bin/bash

exec sudo python3.6 stock_data_downloader/main.py -s update.yaml -i ignore_list.txt update
