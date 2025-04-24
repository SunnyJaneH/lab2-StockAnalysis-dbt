-- Model: transform_stock_metrics
-- Description: Computes price change %, moving averages, volume averages, and RSI(14) from clean_stock_data.

WITH price_changes AS (
    SELECT
        symbol,
        date,
        close,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS price_diff
    FROM {{ ref('clean_stock_data') }}
),

gains_losses AS (
    SELECT
        symbol,
        date,
        CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END AS gain,
        CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END AS loss
    FROM price_changes
),

avg_gains_losses AS (
    SELECT
        symbol,
        date,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
    FROM gains_losses
),

rsi_calc AS (
    SELECT
        symbol,
        date,
        CASE
            WHEN avg_loss_14 = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain_14 / NULLIF(avg_loss_14, 0))))
        END AS rsi_14
    FROM avg_gains_losses
),

metrics AS (
    SELECT
        symbol,
        date,
        close,
        ((close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0)) * 100 AS price_change_pct,
        high - low AS high_low_diff,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS moving_avg_20d,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS moving_avg_50d,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS moving_avg_100d,
        AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS volume_avg_7d,
        updated_at
    FROM {{ ref('clean_stock_data') }}
)

SELECT
    m.symbol,
    m.date,
    m.close,
    m.price_change_pct,
    m.high_low_diff,
    m.moving_avg_20d,
    m.moving_avg_50d,
    m.moving_avg_100d,
    m.volume_avg_7d,
    r.rsi_14,
    m.updated_at
FROM metrics m
LEFT JOIN rsi_calc r
  ON m.symbol = r.symbol AND m.date = r.date