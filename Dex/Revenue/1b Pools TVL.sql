SELECT date_trunc('day', signed_at) AS date
       , sum(
            sum(amount0_unscaled/power(10, prices0.num_decimals)*prices0.price_in_usd) -- USD amount of Token0
		  + sum(amount1_unscaled/power(10, prices1.num_decimals)*prices1.price_in_usd) -- USD amount of Token1
		) OVER (ORDER BY date) AS TVL
       , 'WETH<>USDT' AS pair_ticker
FROM (
    --Deposits (+)
    SELECT signed_at
        , token0_address
        , amount0_unscaled
        , token1_address
        , amount1_unscaled
    FROM reports.dex
    WHERE amount0_unscaled != 0 
        AND amount1_unscaled != 0 
        AND event = 'add_liquidity'
        AND pair_address = unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
        AND chain_id = 42161
        AND protocol_name = 'sushiswap'
        AND version = 2

    UNION ALL

    --Withdrawals (-)
    SELECT signed_at
        , token0_address
        , -1.0*amount0_unscaled AS amount0_unscaled
        , token1_address 
        , -1.0*amount1_unscaled AS amount1_unscaled
    FROM reports.dex
    WHERE amount0_unscaled != 0 
        AND amount1_unscaled != 0
        AND event = 'remove_liquidity'
        AND pair_address = unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
        AND chain_id = 42161
        AND protocol_name = 'sushiswap'
        AND version = 2

    UNION ALL 

    --Withdrawals(-) + Deposits(+)
    SELECT signed_at
        , token0_address 
        , amount0_unscaled
        , token1_address
        , amount1_unscaled
    FROM reports.dex
    WHERE amount0_unscaled != 0 
        AND amount1_unscaled != 0
        AND event = 'swap'
        AND pair_address = unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
        AND chain_id = 42161
        AND protocol_name = 'sushiswap'
        AND version = 2
) data
LEFT JOIN ( -- Metadata for Token0
			      SELECT contract_address, dt, price_in_usd, num_decimals
			      FROM reports.token_prices prices
			      WHERE chain_id = 42161
			       ) prices0
			            ON upper(prices0.contract_address) = hex(token0_address)
			                AND prices0.dt = (SELECT max(dt) from reports.token_prices)
			LEFT JOIN ( -- Metadata for Token1
			      SELECT contract_address, dt, price_in_usd, num_decimals
			      FROM reports.token_prices prices
			      WHERE chain_id = 42161
			       ) prices1
			            ON upper(prices1.contract_address) = hex(token1_address)
			                AND prices1.dt = (SELECT max(dt) from reports.token_prices)
GROUP BY date
ORDER BY date