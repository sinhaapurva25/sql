--3/ How would you use a window function (e.g., ROW_NUMBER()/RANK()/DENSE_RANK() ) to de-duplicate data and keep only the latest record per user?

-- ============================================================================
-- SAMPLE DATA
-- ============================================================================

CREATE TABLE user_activity (
    user_id INT,
    activity_date DATE,
    activity_type VARCHAR(50),
    amount DECIMAL(10,2)
);

INSERT INTO user_activity VALUES
(1, '2025-01-10', 'Login', 0),
(1, '2025-01-15', 'Purchase', 100),
(1, '2025-01-20', 'Purchase', 50),     -- Latest for user 1
(2, '2025-01-12', 'Login', 0),
(2, '2025-01-18', 'Purchase', 200),    -- Latest for user 2
(3, '2025-01-05', 'Purchase', 75),
(3, '2025-01-15', 'Login', 0),
(3, '2025-01-22', 'Purchase', 125);    -- Latest for user 3

-- ============================================================================
-- SOLUTION 1: Using ROW_NUMBER() - BEST CHOICE FOR DE-DUPLICATION
-- ============================================================================
-- ROW_NUMBER() assigns a unique sequential number to each row within a partition
-- Perfect for keeping exactly one record per user (the latest)

SELECT 
    user_id,
    activity_date,
    activity_type,
    amount
FROM (
    SELECT 
        user_id,
        activity_date,
        activity_type,
        amount,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS rn
    FROM user_activity
) AS ranked_data
WHERE rn = 1;

-- Result:
-- user_id | activity_date | activity_type | amount
-- 1       | 2025-01-20    | Purchase      | 50
-- 2       | 2025-01-18    | Purchase      | 200
-- 3       | 2025-01-22    | Purchase      | 125

-- Explanation:
-- • PARTITION BY user_id: Groups rows by user
-- • ORDER BY activity_date DESC: Orders within each group (latest first)
-- • ROW_NUMBER() assigns 1 to the latest, 2 to second latest, etc.
-- • WHERE rn = 1: Filters to keep only the latest record

-- ============================================================================
-- SOLUTION 2: Using RANK()
-- ============================================================================
-- RANK() gives the same rank to tied values, then skips ranks
-- Useful if multiple records have the same latest date

SELECT 
    user_id,
    activity_date,
    activity_type,
    amount
FROM (
    SELECT 
        user_id,
        activity_date,
        activity_type,
        amount,
        RANK() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS rnk
    FROM user_activity
) AS ranked_data
WHERE rnk = 1;

-- Result: Same as ROW_NUMBER() if no ties exist
-- But if user 1 had two records on 2025-01-20:
--   user_id | activity_date | rnk
--   1       | 2025-01-20    | 1
--   1       | 2025-01-20    | 1     ← Both get rank 1
--   1       | 2025-01-15    | 3     ← Next rank is 3 (gap)

-- ============================================================================
-- SOLUTION 3: Using DENSE_RANK()
-- ============================================================================
-- DENSE_RANK() gives the same rank to tied values, but no gaps
-- Similar to RANK() for this use case, but handles ties differently

SELECT 
    user_id,
    activity_date,
    activity_type,
    amount
FROM (
    SELECT 
        user_id,
        activity_date,
        activity_type,
        amount,
        DENSE_RANK() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS drnk
    FROM user_activity
) AS ranked_data
WHERE drnk = 1;

-- Result: Same as above if filtering by = 1

-- ============================================================================
-- COMPARISON: ROW_NUMBER() vs RANK() vs DENSE_RANK()
-- ============================================================================

SELECT 
    user_id,
    activity_date,
    activity_type,
    amount,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS row_num,
    RANK() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS rnk,
    DENSE_RANK() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS dense_rnk
FROM user_activity
ORDER BY user_id, activity_date DESC;

-- ============================================================================
-- RECOMMENDATION: ROW_NUMBER() FOR DE-DUPLICATION
-- ============================================================================

-- Why ROW_NUMBER() is best for de-duplication:
-- 1. Guarantees exactly ONE row per user (no ties)
-- 2. Simple logic: WHERE row_num = 1 always returns one record per partition
-- 3. Predictable: If dates tie, it picks arbitrarily (but consistently)
-- 4. Efficient: No need to handle multiple results

-- Use RANK() or DENSE_RANK() only if you specifically need to:
-- • Handle tied values specially
-- • Keep all records with the latest date if there are multiple

-- ============================================================================
-- COMMON PATTERNS FOR DE-DUPLICATION
-- ============================================================================

-- Pattern 1: Keep latest record per user (by date)
SELECT user_id, activity_date, activity_type, amount
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS rn
    FROM user_activity
) t
WHERE rn = 1;

-- Pattern 2: Keep latest record per user (by date, then by ID for tie-breaking)
SELECT user_id, activity_date, activity_type, amount
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id 
                          ORDER BY activity_date DESC, user_id DESC) AS rn
    FROM user_activity
) t
WHERE rn = 1;

-- Pattern 3: Keep latest N records per user
SELECT user_id, activity_date, activity_type, amount
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS rn
    FROM user_activity
) t
WHERE rn <= 3;  -- Last 3 records per user

-- Pattern 4: De-duplicate on multiple columns, keep latest by date
SELECT user_id, activity_type, activity_date, amount
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id, activity_type 
                          ORDER BY activity_date DESC) AS rn
    FROM user_activity
) t
WHERE rn = 1;

-- ============================================================================
-- REAL-WORLD EXAMPLE: Customer orders
-- ============================================================================

-- Find the latest order for each customer
SELECT 
    customer_id,
    order_date,
    order_amount,
    status
FROM (
    SELECT 
        customer_id,
        order_date,
        order_amount,
        status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
) recent_orders
WHERE rn = 1;

-- ============================================================================
-- USING CTE (Common Table Expression) - CLEANER SYNTAX
-- ============================================================================

WITH ranked_data AS (
    SELECT 
        user_id,
        activity_date,
        activity_type,
        amount,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS rn
    FROM user_activity
)
SELECT 
    user_id,
    activity_date,
    activity_type,
    amount
FROM ranked_data
WHERE rn = 1;

-- ============================================================================
-- KEY TAKEAWAYS
-- ============================================================================
-- • ROW_NUMBER() is the go-to function for de-duplication
-- • Always use PARTITION BY to group by the identifier (user_id)
-- • Always use ORDER BY to determine which record to keep (usually by date DESC)
-- • WHERE condition = 1 filters to keep only one record per group
-- • Use CTE for cleaner, more readable queries
-- ============================================================================