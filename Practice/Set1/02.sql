--2/ Given two tables (Orders and Customers), write a query to find customers who placed no orders in the last year.

orders:
| order_id | customer_id | order_date | amount  |
| -------- | ----------- | ---------- | ------- |
| 101      | 1           | 2025-01-10 | 2500.00 |
| 102      | 1           | 2024-03-05 | 1800.00 |
| 103      | 2           | 2025-11-20 | 3200.00 |
| 104      | 3           | 2025-08-15 | 1500.00 |
| 105      | 4           | 2025-02-28 | 2100.00 |
| 105      | 4           | 2025-02-26 | 2100.00 |

customers:
| customer_id | customer_name | signup_date |
| ----------- | ------------- | ----------- |
| 1           | Amit Sharma   | 2022-01-10  |
| 2           | Neha Gupta    | 2022-05-15  |
| 3           | Rahul Verma   | 2023-02-20  |
| 4           | Priya Singh   | 2023-08-01  |
| 5           | Rohit Mehta   | 2024-01-05  |

-- customers left join orders
| 1           | Amit Sharma   | 2025-01-10  |
| 1           | Amit Sharma   | 2024-03-05  |
| 2           | Neha Gupta    | 2025-11-20  |
| 3           | Rahul Verma   | 2025-08-15  |
| 4           | Priya Singh   | 2025-02-28  |
| 4           | Priya Singh   | 2025-02-26  |
| 5           | Rohit Mehta   | null        |

| 1           | Amit Sharma   | 2025-01-10  |
| 2           | Neha Gupta    | 2025-11-20  |
| 3           | Rahul Verma   | 2025-08-15  |
| 4           | Priya Singh   | 2025-02-28, 2025-02-26  |

| 1           | Amit Sharma   | 2024-03-05  |
| 5           | Rohit Mehta   | null        |


select c.customer_id, c.customer_name, 
from customers c
where not exists (select 1 from orders o
                  WHERE o.customer_id = c.customer_id
                  AND o.order_date >= CURRENT_DATE - INTERVAL '1' YEAR);