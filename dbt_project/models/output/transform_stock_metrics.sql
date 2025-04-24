-- Step 1: Calculate daily price difference for each stock
WITH price_changes AS (
    SELECT
        symbol,
        date,
        close,
        -- Compute the difference between today's close price and previous day's close price
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS price_diff
    FROM {{ ref('clean_stock_data') }}
),

-- Step 2: Determine gains and losses for RSI computation
gains_losses AS (
    SELECT
        symbol,
        date,
        -- If price increased, store the difference as 'gain', otherwise set it to 0
        CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END AS gain,
        -- If price decreased, store the absolute difference as 'loss', otherwise set it to 0
        CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END AS loss
    FROM price_changes
),

-- Step 3: Compute the 14-day average gain and loss for RSI
avg_gains_losses AS (
    SELECT
        symbol,
        date,
        -- Calculate the 14-day average gain for each stock
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        -- Calculate the 14-day average loss for each stock
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
    FROM gains_losses
),

-- Step 4: Compute the RSI(14) indicator
rsi_calc AS (
    SELECT
        symbol,
        date,
        -- Calculate Relative Strength Index (RSI) based on the average gain and loss
        CASE
            WHEN avg_loss_14 = 0 THEN 100  -- If there's no loss, RSI is 100
            ELSE 100 - (100 / (1 + (avg_gain_14 / NULLIF(avg_loss_14, 0))))
        END AS rsi_14
    FROM avg_gains_losses
),

-- Step 5: Compute stock metrics such as price change percentage, moving averages, and volume averages
metrics AS (
    SELECT
        symbol,
        date,
        close,
        -- Compute price change percentage compared to previous day
        ((close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0)) * 100 AS price_change_pct,
        -- Calculate the difference between the day's high and low prices
        high - low AS high_low_diff,
        -- Compute 20-day moving average for closing price
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS moving_avg_20d,
        -- Compute 50-day moving average for closing price
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS moving_avg_50d,
        -- Compute 200-day moving average for closing price
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS moving_avg_200d,
        -- Compute 7-day moving average for volume
        AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS volume_avg_7d,
        updated_at
    FROM {{ ref('clean_stock_data') }}
)

-- Step 6: Combine metrics with RSI calculations and order results by symbol and date
SELECT
    m.symbol,
    m.date,
    m.close,
    m.price_change_pct,
    m.high_low_diff,
    m.moving_avg_20d,
    m.moving_avg_50d,
    m.moving_avg_200d,
    m.volume_avg_7d,
    r.rsi_14,
    m.updated_at
FROM metrics m
LEFT JOIN rsi_calc r
  ON m.symbol = r.symbol AND m.date = r.date
ORDER BY m.symbol, m.date