WITH contracts AS (
    SELECT
        contract_address,
        token_addresses
    FROM
        ethereum.dex_pools
    WHERE
        exchange_name = 'uniswap'
        AND contract_version = 'v3'
),
latest_liquidity AS (
    SELECT
        contract_address,
        liquidity,
        ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY contract_address,
            timestamp DESC) AS rank
    FROM
        ethereum.dex_liquidity
    WHERE
        TIMESTAMP >= '2023-03-06'
),
contracts_by_liquidity AS (
    SELECT
        r.contract_address,
        token_addresses,
        ROW_NUMBER() OVER (PARTITION BY token_addresses ORDER BY liquidity DESC) AS liquidity_rank
    FROM
        latest_liquidity r
        INNER JOIN contracts c ON c.contract_address = r.contract_address
    WHERE
        rank = 1
        AND liquidity IS NOT NULL
)
SELECT
    contract_address,
    token_addresses
FROM
    contracts_by_liquidity
WHERE
    liquidity_rank = 1
    AND (token_addresses[1] ILIKE '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    AND (token_addresses[2] IN (
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
	'0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
	'0xdac17f958d2ee523a2206206994597c13d831ec7',
	'0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
	'0x4206931337dc273a630d328da6441786bfad668f',
	'0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0',
	'0x4fabb145d64652a948d72533023f6e7a623c7c53',
	'0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'))
    OR token_addresses[2] ILIKE '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    AND (token_addresses[1] IN (
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
	'0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
	'0xdac17f958d2ee523a2206206994597c13d831ec7',
	'0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
	'0x4206931337dc273a630d328da6441786bfad668f',
	'0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0',
	'0x4fabb145d64652a948d72533023f6e7a623c7c53',
	'0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'))
)
