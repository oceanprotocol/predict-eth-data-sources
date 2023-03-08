from jinja2 import Environment, FileSystemLoader
from fastapi import HTTPException

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

    def retrieve_liquidity(self, start_date, end_date, timeframe):
        query_template = self.jenv.get_template("liquidity.sql")
        tf = self._transform_timeframe(timeframe)
        query = query_template.render(
            start_date=start_date,
            end_date=end_date,
            timeframe=tf,
            token_addresses=list(UNISWAP_TOKEN_ADDRESSES.keys()),
            contract_addresses=list(UNISWAP_CONTRACT_ADDRESSES.keys()),
        )
        response = requests.post(
            TransposeService.URL,
            headers = self.headers,
            json = {
                "sql": query,
            },
        )
        print(query)

        if response.status_code == 200:
            df = pd.DataFrame(response.json()["results"])
            df.index = pd.to_datetime(df["datetime"]).copy()
            df.drop(["datetime"], axis=1, inplace=True)
            df["contract_symbol"] = df["contract_address"].apply(lambda row: UNISWAP_CONTRACT_ADDRESSES[row.lower()])
            df.drop(["contract_address"], axis=1, inplace=True)

            return df
        else:
            raise HTTPException(status_code=response.status_code, detail=response.json())

    def retrieve_swap_volumes(self, start_date, end_date, timeframe):
        query_template = self.jenv.get_template("swaps_volume.sql")
        tf = self._transform_timeframe(timeframe)
        query = query_template.render(start_date=start_date, end_date=end_date, timeframe=tf)

        response = requests.post(
            TransposeService.URL,
            headers = self.headers,
            json = {"sql": query},
        )
        
        df = pd.DataFrame(response.json()["results"])
        df.index = df["datetime"].copy()
        df.drop(["datetime"], axis=1, inplace=True)

        return df
