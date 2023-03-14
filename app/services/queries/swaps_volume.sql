SELECT 
    DATE_TRUNC('{{ timeframe }}', timestamp) AS datetime,
    contract_address,
    SUM(quantity_in) AS volume_in, 
    SUM(quantity_out) AS volume_out
FROM ethereum.dex_swaps 
WHERE TIMESTAMP BETWEEN '{{ start_date }}' AND '{{ end_date }}' AND
    exchange_name = 'uniswap' AND
    contract_version = 'v3' AND
    contract_address IN ({% for address in contract_addresses %}
             '{{ address }}' {{ ',' if not loop.last else '' }}
    {%- endfor %})
GROUP BY datetime, contract_address
ORDER BY datetime, contract_address
