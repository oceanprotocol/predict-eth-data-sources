from datetime import datetime
from datetime import timezone
from typing import Optional

from services.data import DataService
from services.transpose import TransposeService
from fastapi import APIRouter, Depends

import json

router = APIRouter()

@router.get("/ccxt/price-history/ohlcv")
def currency_data(
        start_date: datetime,
        timeframe: str,
        end_date: Optional[datetime] = datetime.utcnow().replace(tzinfo=timezone.utc),
        data_service: TransposeService = Depends(DataService),
):
    df = data_service.retrieve_ohlcv(start_date, end_date, timeframe)
    
    return json.loads(df.to_json(orient="table"))

@router.get("/transpose/exchange-liquidity/uniswap")
def exchange_data_liquidity(
        start_date: datetime,
        timeframe: str,
        end_date: Optional[datetime] = datetime.utcnow().replace(tzinfo=timezone.utc),
        transpose_service: TransposeService = Depends(TransposeService),
):
    df = transpose_service.retrieve_liquidity(start_date, end_date, timeframe)
    return json.loads(df.to_json(orient="table"))

@router.get("/transpose/exchange-volume/uniswap")
def exchange_swap_volume(
        start_date: datetime,
        timeframe: str,
        end_date: Optional[datetime] = datetime.utcnow().replace(tzinfo=timezone.utc),
        transpose_service: TransposeService = Depends(TransposeService),
):
    df = transpose_service.retrieve_swap_volumes(start_date, end_date, timeframe)
    return json.loads(df.to_json(orient="table"))

@router.get("/merged")
def merged_dataset(
        start_date: datetime,
        timeframe: str,
        end_date: Optional[datetime] = datetime.utcnow().replace(tzinfo=timezone.utc),
        data_service: TransposeService = Depends(DataService),
        transpose_service: TransposeService = Depends(TransposeService),
):
    prices = data_service.retrieve_ohlcv(start_date, end_date, timeframe)
    liquidity = transpose_service.retrieve_liquidity(start_date, end_date, timeframe)

    merged = prices.merge(
        liquidity, left_on=["date", "symbol"], right_on=["datetime", "contract_symbol"], how="left", validate="one_to_one"
    )
    merged.drop(["contract_symbol"], axis=1, inplace=True)

    return json.loads(merged.to_json(orient="table"))
