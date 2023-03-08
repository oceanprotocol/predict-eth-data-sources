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
        unformatted_response = []
        formatted_response = []
        current_date = start_date

        for symbol in TOP_20_SYMBOLS:

            #limit to 200 days for now because of the exchange
            while (current_date <= end_date):

                if (end_date - current_date).days < 200:
                    params = {"until": int(end_date.timestamp() * 1000)}
                else:
                    params = {}

                unformatted_response += self.exchange.fetch_ohlcv(
                    symbol + "/USD",
                    timeframe=timeframe,
                    since=int(current_date.timestamp() * 1000),
                    limit=200,
                    params=params,
                )

                current_date = datetime.fromtimestamp(
                    unformatted_response[-1][0] / 1000, timezone.utc
                ) + timedelta(seconds=seconds)

            # transform data
            df = pd.DataFrame(unformatted_response, columns=["date", "open", "high", "low", "close", "volume"])
            df.index = df["date"].apply(lambda timestamp: datetime.fromtimestamp(timestamp / 1000, pytz.utc))
            df.drop(["date"], axis=1, inplace=True)
            df["symbol"] = symbol + "/USDC"

            formatted_response.append(df)

        return pd.concat(formatted_response)

