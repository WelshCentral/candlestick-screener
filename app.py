import os, csv
import talib
import yfinance as yf
import pandas
from dateutil.parser import parse
from datetime import date
from datetime import time
from datetime import datetime
from datetime import timedelta
from flask import Flask, escape, request, render_template
from patterns import candlestick_patterns

app = Flask(__name__)

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

# @app.route('/snapshot')
# def snapshot():
#     with open('datasets/symbols.csv') as f:
#         for line in f:
#             if "," not in line:
#                 continue
#             symbol = line.split(",")[0]
#             data = yf.download(symbol, start="2020-12-01", end="2021-12-10")
#             data.to_csv('datasets/daily/{}.csv'.format(symbol))

#     return {
#         "code": "success"
#     }



@app.route('/snapshot')
def snapshot():
    with open('datasets/symbols.csv') as f:
        today = date.today()
        print('today: ', today)
        one_year = today - timedelta(days=365)
        print('one year: ', one_year)
        companies = f.read().splitlines()
    

        for n, company in enumerate(companies):
            symbol = company.split(',')[0]
            symbol = symbol.replace('.', '') # yfinance doesn't handle non alphabetical characters - eg. brookshire 'BKR.B' must be 'BKRB'
            remaining_tickers = len(companies)
            path = 'datasets/daily/{}.csv'.format(symbol)

            # check if files exist in directory
            if os.path.exists(path):
                remaining_tickers -= 1

                # open files and check last download date
                with open(path) as f:
                    data = f.read().splitlines()
                    
                    if is_date(data[-1].split(',')[0]):
                        last_download_date = datetime.strptime(data[-1].split(',')[0], '%Y-%m-%d').date()

                        # check last download date and compare to today
                        if today != last_download_date:
                            df = yf.download(symbol, start = last_download_date + timedelta(days = 1), end = today + timedelta(days = 1))
                            df_time = df.index[0].date()

                            # if new data available then proceed with appending
                            if last_download_date != df_time:
                                print('')
                                print('last update for ' "{}" ' was:'.format(symbol), last_download_date)
                                print('updating:', symbol,'      downloads remaining:', remaining_tickers - n)
                                print('-------------------------------------------------------------------------')
                                print('')

                                df.to_csv(path, mode = 'a', header = False)

                            else:
                                print(symbol + ' is already up to date')

                    else:
                        print('')
                        print('error finding date: ', symbol,'  downloads remaining:', remaining_tickers - n)
        
            # if not original file then proceed with fresh download
            else:
                print('')
                print('')
                print('downloading: ', symbol,'  downloads remaining:', remaining_tickers - n)
                df = yf.download(symbol, start = one_year, end = today + timedelta(days = 1))
                df.to_csv('datasets/daily/{}.csv'.format(symbol))

    return {
        'code': 'success'
    }


@app.route('/')
def index():
    pattern  = request.args.get('pattern', False)
    stocks = {}

    with open('datasets/symbols.csv') as f:
        for row in csv.reader(f):
            stocks[row[0]] = {'company': row[1]}

    if pattern:
        for filename in os.listdir('datasets/daily'):
            df = pandas.read_csv('datasets/daily/{}'.format(filename))
            pattern_function = getattr(talib, pattern)
            symbol = filename.split('.')[0]

            try:
                results = pattern_function(df['Open'], df['High'], df['Low'], df['Close'])
                last = results.tail(1).values[0]

                if last > 0:
                    stocks[symbol][pattern] = 'bullish'
                elif last < 0:
                    stocks[symbol][pattern] = 'bearish'
                else:
                    stocks[symbol][pattern] = None
            except Exception as e:
                print('failed on filename: ', filename)

    return render_template('index.html', candlestick_patterns=candlestick_patterns, stocks=stocks, pattern=pattern)


app.run()