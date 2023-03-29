SELECT
    [signed_at:aggregation] AS date,
    'WETH<>USDT' AS pair_ticker,
    sum(abs(amount0_unscaled)/power(10, prices.num_decimals)*prices.price_in_usd) AS volume
FROM reports.dex dex
LEFT JOIN (
        SELECT dt, contract_address, price_in_usd, num_decimals
        FROM reports.token_prices 
		WHERE chain_id = 42161
	) prices ON hex(dex.token0_address) = upper(prices.contract_address)
				AND date_trunc('day', dex.signed_at) = prices.dt
WHERE chain_id = 42161
    AND [signed_at:daterange]
    AND pair_address = unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
	AND event = 'swap'
GROUP BY date