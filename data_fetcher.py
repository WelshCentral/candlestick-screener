import pandas as pd
from ally import AllyAPI
from ally.requests import QuotesRequest
import datetime
import time

class DataFetcher(object):
    def __init__(self, ticker_filename, expiration_date, collateral, strikes_out=1):
        """
            ticker_filename -> filename for tickers to be considered (csv)
            expiration_date -> expiration date of the options considered
            collateral -> amount of funds available as collateral
            strikes_out -> how many strikes below current price
                - note that this is only for Puts for Calls options are
                  traded on the assigned shares
        """
        self.filename = ticker_filename
        self.expiration = expiration_date
        self.strikes_out = strikes_out
        self.collateral = collateral

        self.CONSUMER_KEY = "your Ally Invest consumer key"
        self.OAUTH_TOKEN = "your Ally Invest OAUTH token"
        self.OAUTH_SECRET = "your Ally Invest OAUTH secret"

        self.ally = AllyAPI(self.OAUTH_SECRET, self.OAUTH_TOKEN, self.CONSUMER_KEY, response_format='json')

    def fetch_data(self):
        ticker_csv = pd.read_csv(self.filename)

        # get stock quotes for all tickers; keep price, symbol, and beta measurement
        data = []
        tickers_per_request = 400
        tickers = ticker_csv["Ticker"].tolist()
        tickers = [tickers[x:x+tickers_per_request] for x in range(0, len(tickers), tickers_per_request)]
        for ticker_list in tickers:
            quote_request = QuotesRequest(symbols=ticker_list)
            response = quote_request.execute(self.ally)
            for quote in response.get_quotes():
                # bad fetch or too expensive or bankrupt
                if quote.symbol == 'na' or float(quote.last)*100 > self.collateral \
                        or float(quote.bid) == 0 or float(quote.ask) == 0 or quote.beta == '':
                    continue
                data.append([quote.symbol, ((float(quote.ask) + float(quote.bid))/2.0), float(quote.beta)])

        # get available strike prices for all tickers (ally.get_options_strikes(symbol))
        count = 0
        for dp in data:
            if count == 100:    # this isn't the rate limit but there are other processes using the API
                print("Rate limit reached, sleeping for 15 seconds...")
                time.sleep(15)
                count = 0
            js = self.ally.get_options_strikes(dp[0])   # unfortunately, this can't take multiple tickers
            strikes = js['response']['prices']['price']
            # get index of nearest strike less than current price
            idx = next((x for x, val in enumerate(strikes) if float(val) > dp[1]), 0) - self.strikes_out
            if idx < 0:
                dp.append(0)
                continue
            strike_price = float(strikes[idx])
            dp.append(strike_price)
            count += 1

        tickers_per_request = 100
        tickers = ticker_csv["Ticker"].tolist()
        tickers = [tickers[x:x+tickers_per_request] for x in range(0, len(tickers), tickers_per_request)]
        for ticker_list in tickers:
            putcall, dates = [], []
            for _ in range(len(ticker_list)):
                putcall.append("p")
                dates.append(self.expiration)

            tickers, strikes = [], []
            for ticker in ticker_list:
                for dp in data:
                    if dp[0] == ticker:
                        tickers.append(ticker)
                        strikes.append(dp[3])
                        break

            quote = self.ally.get_option_quote(tickers, dates, strikes, putcall)['response']['quotes']['quote']
            for q in quote:
                for dp in data:
                    if dp[0] == q['undersymbol']:
                        dp.append(((float(q['ask']) + float(q['bid']))/2))
                        break

        return  pd.DataFrame(data, columns=['symbol', 'price_mid', 'beta', 'option_strike', 'option_income'])

        
if __name__ == '__main__':
    fetcher = DataFetcher('weekly_option_tickers.csv', datetime.datetime(2021, 3, 5), 250000, strikes_out=1)
    data = fetcher.fetch_data() 