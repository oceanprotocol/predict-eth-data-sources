WITH ranked_records AS (
    SELECT
        DATE_TRUNC('{{ timeframe }}', timestamp) AS datetime,
        token_address,
	contract_address,
	liquidity,
        ROW_NUMBER() OVER (
            PARTITION BY contract_address, DATE_TRUNC('{{ timeframe }}', timestamp
        ) ORDER BY token_address, contract_address, timestamp DESC) AS rank
    FROM ethereum.dex_liquidity
    WHERE 
        timestamp BETWEEN '{{ start_date }}' AND '{{ end_date }}' AND
        token_address IN ({% for address in token_addresses %}
             '{{ address }}' {{ ',' if not loop.last else '' }}
    	{%- endfor %}) AND
        contract_address IN ({% for address in contract_addresses %}
             '{{ address }}' {{ ',' if not loop.last else '' }}
    	{%- endfor %}) AND
        category IN ('deposit', 'withdraw') AND
        exchange_name = 'uniswap' AND
        contract_version = 'v3'
)
SELECT DISTINCT datetime, contract_address, liquidity
FROM ranked_records
WHERE rank = 1
