with user_cohorts as (
    SELECT  taker as address
            , min(date_trunc('month', signed_at)) as cohortMonth
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
        AND market = 'aavegotchi'
    GROUP BY address
),
following_months as (
    SELECT  taker as address
            , date_diff('month', uc.cohortMonth, date_trunc('month', signed_at))  as month_number
    FROM reports.nft_sales_all_chains
    LEFT JOIN user_cohorts uc ON address = uc.address
    WHERE chain_name = 'matic_mainnet'
        AND market = 'aavegotchi'
    GROUP BY address, month_number
),
cohort_size as (
    SELECT  uc.cohortMonth as cohortMonth
            , count(*) as num_users
    FROM user_cohorts uc
    GROUP BY cohortMonth
    ORDER BY cohortMonth
),
retention_table as (
    SELECT  c.cohortMonth as cohortMonth
            , o.month_number as month_number
            , count(*) as num_users
    FROM following_months o
    LEFT JOIN user_cohorts c ON o.address = c.address
    GROUP BY cohortMonth, month_number
)
SELECT  r.cohortMonth
        , s.num_users as new_buyers
        , r.month_number
        , r.num_users / s.num_users as retention
FROM retention_table r
LEFT JOIN cohort_size s 
	ON r.cohortMonth = s.cohortMonth
WHERE r.month_number != 0
ORDER BY r.cohortMonth, r.month_number