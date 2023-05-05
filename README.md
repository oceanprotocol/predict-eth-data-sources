# Exchange Data Provider

Utilizing this API enables the gathering of OHLCV prices for various cryptocurrencies including BTC, ETH, USDT, BNB, USDC, XRP, ADA, DOGE, MATIC, BUSD, SOL, DOT, LTC, SHIB, TRX, AVAX, DAI, UNI, LINK, and WBTC from Binance. Additionally, the API provides information on liquidity, volumes, and gas price data extracted from USDC pools on Uniswap.

## Description

The purpose of developing this API is to supply features to models that aim to forecast the future performance of cryptocurrencies.

The application uses ccxt and transpose to fetch data.

## Getting Started

### Dependencies

* Poetry version 1.4.0
* Python version 3.8

### Installing

The following steps lead to the installation of the project:

```
$ git clone <ocean_repo>
$ cd <ocean_repo>
$ poetry install
$ poetry run python app/app.py
```

Then application will launch waiting for incoming requests.

### Environment Variables

This API uses Transpose under the hood, so an API KEY is needed for the application to run. You can either set up the environment variable `TRANSPOSE_API_KEY` before running the app or put it in an `.env` file since the application will map directly the file.

### Endpoints & Usage

The API can be accessed at https://predict-eth-data.oceanprotocol.com. The following endpoints are created in the API:

* `/ccxt/price-history/ohlcv` --> Retrieves only OHLCV data
* `/transpose/exchange-liquidity/uniswap` --> Retrieves only liquidity data
* `/transpose/exchange-volume/uniswap` --> Retrieves only volume data
* `/merged` --> Retrieves all data merged

All the endpoints receive three query params which are:

* `start_date`: encoded string in isoformat
* `end_date` [Optional]: encoded string in isoformat, it uses current date if not included
* `timeframe`: the frequency of the resampling as a string, it should contain one of the following: 1d, 1h, 1m, 1s.

The following is an example cURL:
```
$ curl --get --data-urlencode "start_date=2023-03-06T15:53:00+05:00"  "https://predict-eth-data.oceanprotocol.com/merged?timeframe=1h"
```


## Version History

* 0.1.0
    * Initial Release
