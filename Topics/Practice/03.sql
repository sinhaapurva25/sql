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
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date DESC) AS rnk
    FROM user_activity
) AS ranked_data
WHERE rnk = 1;

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
