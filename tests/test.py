
from database import Database

db = Database('test')

last_date = db.get_last_timestamp('aapl')

if last_date:
    print(last_date)
else:
    print('No data.')
