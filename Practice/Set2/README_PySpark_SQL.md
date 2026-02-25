🌈 𝐒𝐄𝐋𝐄𝐂𝐓
SQL:
 SELECT column(s) FROM table
 SELECT * FROM table
 SELECT a, b, c FROM t

PySpark:
 df.select("column(s)")
 df.select("*")
 df.select("a", "b", "c")

🌈 𝐃𝐈𝐒𝐓𝐈𝐍𝐂𝐓
SQL:
 SELECT DISTINCT column(s) FROM table
 SELECT DISTINCT a FROM t

PySpark:
 df.select("column(s)").distinct()
 df.select("a").distinct()

🌈 𝐖𝐇𝐄𝐑𝐄
SQL:
 SELECT column(s) FROM table WHERE condition
 SELECT \* FROM t WHERE a > 10 AND b = 'x'

PySpark:
 df.filter(condition).select("column(s)")
 df.filter((df.a > 10) \& (df.b == "x"))
 # or
 df.where((col("a") > 10) \& (col("b") == "x"))


🌈 𝐎𝐑𝐃𝐄𝐑 𝐁𝐘/SORT
SQL:
 SELECT column(s) FROM table ORDER BY column(s)
 SELECT * FROM t ORDER BY a DESC, b ASC

PySpark:
 df.sort("column(s)").select("column(s)")
 df.orderBy(col("a").desc(), col("b").asc())
 # or
 df.sort(col("a").desc(), col("b").asc())

🌈 𝐋𝐈𝐌𝐈𝐓
SQL:
 SELECT column(s) FROM table LIMIT n
 SELECT \* FROM t LIMIT 10

PySpark:
 df.limit(n).select("column(s)")
 df.limit(10)

🌈 𝐂𝐎𝐔𝐍𝐓
SQL:
 SELECT COUNT(*) FROM table
 SELECT COUNT(a), SUM(b), AVG(c) FROM t

PySpark:
 df.count()
 df.agg(
    count("a"),
    sum("b"),
    avg("c")
)

🌈 GROUP BY
SQL:
    SELECT a, COUNT(*) FROM t GROUP BY a
    SELECT a, SUM(b) FROM t GROUP BY a

PySpark:
    df.groupBy("a").count()
    df.groupBy("a").agg(sum("b"))

🌈 HAVING
SQL:
SELECT a, COUNT(*) 
FROM t 
GROUP BY a
HAVING COUNT(*) > 10

PySpark:
df.groupBy("a").count().filter(col("count") > 10)

🌈 JOIN
SQL:
SELECT *
FROM t1
JOIN t2 ON t1.id = t2.id

PySpark:
df1.join(df2, df1.id == df2.id, "inner")

SQL:
LEFT JOIN / RIGHT JOIN / FULL JOIN

PySpark:
df1.join(df2, "id", "left")
df1.join(df2, "id", "right")
df1.join(df2, "id", "outer")

🌈 COLUMN EXPRESSIONS

SQL:
SELECT a + b AS c FROM t

PySpark:
df.select((col("a") + col("b")).alias("c"))

🌈 CASE WHEN

SQL:
SELECT 
  CASE 
    WHEN a > 10 THEN 'big'
    ELSE 'small'
  END AS size
FROM t

PySpark:
from pyspark.sql.functions import when
df.select(
    when(col("a") > 10, "big").otherwise("small").alias("size")
)

🌈 NULL HANDLING

SQL:
SELECT * FROM t WHERE a IS NULL
SELECT * FROM t WHERE a IS NOT NULL
COALESCE(a, 0)

PySpark:
df.filter(col("a").isNull())
df.filter(col("a").isNotNull())
coalesce(col("a"), lit(0))

🌈 IN / BETWEEN / LIKE

SQL:
WHERE a IN (1,2,3)
WHERE a BETWEEN 10 AND 20
WHERE name LIKE '%abc%'

PySpark:
df.filter(col("a").isin(1,2,3))
df.filter(col("a").between(10, 20))
df.filter(col("name").like("%abc%"))

🌈 UNION

SQL:
SELECT * FROM t1
UNION ALL
SELECT * FROM t2

PySpark:
df1.union(df2)

SQL:
UNION (deduplicates)

PySpark:
df1.union(df2).distinct()

🌈 WINDOW FUNCTIONS

SQL:
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn
FROM t

PySpark:
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
w = Window.partitionBy("a").orderBy("b")
df.withColumn("rn", row_number().over(w))

🌈 CREATE / REPLACE COLUMN

SQL:
SELECT a, b, a+b AS c FROM t

PySpark:
df.withColumn("c", col("a") + col("b"))

🌈 DROP COLUMN

SQL:
-- not standard SQL, but conceptually

PySpark:
df.drop("column\_name")

🌈 RENAME COLUMN

SQL:
SELECT a AS new\_a FROM t

PySpark:
df.withColumnRenamed("a", "new\_a")
