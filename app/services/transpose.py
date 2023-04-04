from jinja2 import Environment, FileSystemLoader
from fastapi import HTTPException
from datetime import timedelta
from retry import retry

from constants import UNISWAP_TOKEN_ADDRESSES
from constants import UNISWAP_CONTRACT_ADDRESSES
from config import settings

import pandas as pd
import requests

class TransposeService:

    URL = "https://api.transpose.io/sql"
    TIMEFRAMES_HANDLED = {
        "1d": "day",
        "1h": "hour",
        "1m": "minute",
        "1s": "second",
    }

    def __init__(self, queries_path = ""):
        self.headers = {
            "Content-Type": "application/json",
            "X-API-KEY": settings.transpose_api_key,
        }

        if len(queries_path) != 0:
            self.queries_path = queries_path
        else:
            self.queries_path = "./app/services/queries/"

        self.jenv =  Environment(
            loader=FileSystemLoader(self.queries_path),
        )

    def _transform_timeframe(self, timeframe):
        if timeframe in TransposeService.TIMEFRAMES_HANDLED:
            return TransposeService.TIMEFRAMES_HANDLED[timeframe]
        else:
            raise ValueError("Timeframe not handled.")

    @retry(tries=3, delay=1, backoff=2)
    def _retryable_post(self, url, headers, json):
        return requests.post(
            url,
            headers = headers,
            json = json,
        )

    def retrieve_data_in_batch(self, start_date, end_date, timeframe, query):
        query_template = self.jenv.get_template(query)
        tf = self._transform_timeframe(timeframe)
        current_date = start_date
        seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
        seconds = 15 * seconds_per_unit["d"]
        data = []

        while (current_date <= end_date):
            print("\t", current_date, current_date + timedelta(seconds=seconds))
            query = query_template.render(
                start_date=current_date,
                end_date=current_date + timedelta(seconds=seconds),
                timeframe=tf,
                token_addresses=list(UNISWAP_TOKEN_ADDRESSES.keys()),
                contract_addresses=list(UNISWAP_CONTRACT_ADDRESSES.keys()),
            )

            response = self._retryable_post(
                TransposeService.URL,
                headers = self.headers,
                json = {
                    "sql": query,
                },
            )

            if response.status_code == 200:
                df = pd.DataFrame(response.json()["results"])
                df.index = pd.to_datetime(df["datetime"]).copy()
                df.drop(["datetime"], axis=1, inplace=True)

                data.append(df)

                current_date = current_date + timedelta(seconds=seconds)
            else:
                raise HTTPException(status_code=response.status_code, detail=response.json())

        return pd.concat(data)

    def retrieve_liquidity(self, start_date, end_date, timeframe):
        df = self.retrieve_data_in_batch(
            start_date, end_date, timeframe, "liquidity.sql",
        )

        df["contract_symbol"] = df["contract_address"].apply(lambda row: UNISWAP_CONTRACT_ADDRESSES[row.lower()])
        df.drop(["contract_address"], axis=1, inplace=True)

        return df
        
    def retrieve_swap_volumes(self, start_date, end_date, timeframe):
        df = self.retrieve_data_in_batch(
            start_date, end_date, timeframe, "swaps_volume.sql",
        )

        df["contract_symbol"] = df["contract_address"].apply(lambda row: UNISWAP_CONTRACT_ADDRESSES[row.lower()])
        df.drop(["contract_address"], axis=1, inplace=True)

        return df


    def retrieve_gas_price(self, start_date, end_date, timeframe):
        return self.retrieve_data_in_batch(
            start_date, end_date, timeframe, "gas_price.sql",
        )
