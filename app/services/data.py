from ccxt.base.errors import BadSymbol
from datetime import datetime
from datetime import timezone
from datetime import timedelta

from constants import TOP_20_SYMBOLS

import ccxt
import pytz
import pandas as pd

class DataService:

    def __init__(self):
        self.exchange = ccxt.binance()    

    def retrieve_ohlcv(self, start_date, end_date, timeframe):
        seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
        seconds = int(timeframe[:-1]) * seconds_per_unit[timeframe[-1]]
        formatted_response = []

        for symbol in TOP_20_SYMBOLS:

            unformatted_response = []
            current_date = start_date

            while (current_date <= end_date):

                if (end_date - current_date).days < 200:
                    params = {"until": int(end_date.timestamp() * 1000)}
                else:
                    params = {}

                try:
                    unformatted_response += self.exchange.fetch_ohlcv(
                        symbol + "/USD",
                        timeframe=timeframe,
                        since=int(current_date.timestamp() * 1000),
                        limit=200,
                        params=params,
                    )

                    if len(unformatted_response) == 0:
                        print("Empty response from CCXT: " + symbol + "/USD")
                        break
                except BadSymbol as e:
                    print(e)
                    break
                    

                current_date = datetime.fromtimestamp(
                    unformatted_response[-1][0] / 1000, timezone.utc
                ) + timedelta(seconds=seconds)

                if len(unformatted_response) >= 2000:
                    break

            # transform data
            df = pd.DataFrame(unformatted_response, columns=["date", "open", "high", "low", "close", "volume"])
            df.index = df["date"].apply(lambda timestamp: datetime.fromtimestamp(timestamp / 1000, pytz.utc))
            df.drop(["date"], axis=1, inplace=True)
            df["symbol"] = symbol + "/USDC"

            formatted_response.append(df)

        return pd.concat(formatted_response)

