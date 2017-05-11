
from influxdb import InfluxDBClient


class Database(object):

    def __init__(self, db_name):
        self._client = InfluxDBClient('localhost', 8086, 'root', 'root', db_name)


    def write_row(self, symbol, timestamp, open, high, low, close, volume):
        points = [
            {
                'measurement' : 'stock',
                'tags' :
                {
                    'symbol' : symbol
                },
                'time' : timestamp,
                'fields' :
                {
                    'open'   : open,
                    'high'   : high,
                    'low'    : low,
                    'close'  : close,
                    'volume' : volume
                }
            }
        ]

        self._client.write_points(points)


    def delete_row(self, symbol, date):
        query_str = 'delete from stock where time = \'{0}\' and symbol = \'{1}\''.format(date, symbol)
        self._client.query(query_str)
