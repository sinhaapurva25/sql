1/ Write a SQL query to find the second-highest salary in a table of employees.

2/ Given two tables (Orders and Customers), write a query to find customers who placed no orders in the last year.

3/ How would you use a window function (e.g., ROW_NUMBER()/RANK()/DENSE_RANK() ) to de-duplicate data and keep only the latest record per user?

4/ Explain LEFT, RIGHT, INNER, and FULL OUTER JOINs with their use-cases; then write a scenario-based query.

5/ Given a table of events(event_time, user_id, event_type), write a query to compute session counts per user (e.g., grouping events into sessions by a 30-minute inactivity threshold).

6/ You have a large table of transactions: How would you partition/cluster it to improve query performance over a date range? What considerations apply?

7/ Write a query to find duplicates in a table (same user_id, same timestamp, same event_type) and then delete the duplicates, keeping only one.

8/ What is the difference between a star schema and a snowflake schema in a data warehouse? When would you use each?

9/ How do you handle slowly changing dimensions (Type 1 vs Type 2) in SQL design?

10/ Given a sales table with columns (region, product, sale_date, revenue), write a query to find the top-3 products by revenue per region for each month.

11/ How would you restructure a table to convert it from OLTP to OLAP usage (for analytics), and what SQL changes would you expect?

12/ Given a report requirement: “Show average, median, 95th-percentile of query latency per day per data center”, write a SQL query (or outline it) to produce the metric.

13/ Explain the difference between a clustered index and a non-clustered index, and how they affect query performance in a SQL engine.

14/ Write a SQL query that uses a Common Table Expression (CTE) and explains why using the CTE is beneficial (vs sub-query).

15/ Explain how you’d implement incremental ingestion logic in SQL, e.g., pulling only newly changed records since the last run.