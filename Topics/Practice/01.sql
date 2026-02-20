-- 1/ Write a SQL query to find the second-highest salary in a table of employees.

SELECT name, salary, rnk
FROM (SELECT
        name, salary,
        dense_rank() over (order by salary desc) rnk
        from employees
     ) t
where rnk = 2


-- SOME MORE EXAMPLES

EMPLOYEES
| name | salary |
| ---- | ------ |
| A    | 100    |
| B    | 200    |
| C    | 200    |
| D    | 300    |

-- USAGE OF row_number, rank, dense_rank
select
  name,
  salary,
  row_number() over (order by salary desc) as row_num,
  rank() over (order by salary desc) as rnk,
  dense_rank() over (order by salary desc) as dense_rnk
from employees;

-- display row_number, rank, dense_rank and arrange by row_num
select
  name,
  salary,
  row_number() over (order by salary desc) as row_num,
  rank() over (order by salary desc) as rnk,
  dense_rank() over (order by salary desc) as dense_rnk
from employees
ORDER BY row_num;

-- Find the highest salary in each department
select name, salary, department, rnk
from (select name, salary, department,
      rank() over (partition by department order by salary desc) as rnk
      from employees) t
where rnk=1

--find employees with top 3 salaries
select name, salary, department
from employees
order by salary desc
limit 3;

-- Find the salary difference between highest and second-highest
select (select salary from employees where salary is not null order by salary desc limit 1) - (select salary from (select salary, dense_rank() over(order by salary desc) as dense_rank from employees where salary is not null) t where dense_rank=2) as salary_diff;

-- Find duplicates (employees with the same salary as others)

select name from employees where salary in (select salary from employees
group by salary
having count(*) > 1)