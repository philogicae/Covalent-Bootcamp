SELECT date, Volume, (Volume/Previous_Period)-1 as Growth_Rates
FROM (
    SELECT
        [signed_at:aggregation] as date,
        sum(nft_token_price_usd) as Volume,
        lagInFrame(Volume) OVER (ORDER BY date) as Previous_Period
    FROM reports.nft_sales_all_chains  
    WHERE chain_name = 'matic_mainnet'
        AND [signed_at:daterange]
        AND market = 'aavegotchi'
    GROUP BY date
    ORDER BY date ASC 
)