SELECT
    DATE_TRUNC('{{ timeframe }}', timestamp) AS datetime,
    AVG(base_fee_per_gas) AS gas_price
FROM
    ethereum.blocks
WHERE
    timestamp BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY
    datetime;
