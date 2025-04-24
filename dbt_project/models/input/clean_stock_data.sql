WITH stock_data_cleaning AS (
    SELECT
        symbol,
        CAST(date AS DATE) AS date,  
        open,
        close,
        high,
        low,
        volume,
        updated_at  
    FROM {{ source('raw', 'stock_data') }}
    WHERE close IS NOT NULL  
      AND volume > 0         
      AND high >= low        
),
data_for_analysis AS (
    SELECT
        symbol,
        date,
        close,
        volume,
        high,
        low,
        updated_at,  
        (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS price_change_pct,
        (high - low) AS high_low_diff
    FROM stock_data_cleaning
)

SELECT * 
FROM data_for_analysis
ORDER BY symbol, date