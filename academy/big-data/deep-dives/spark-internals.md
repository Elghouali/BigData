# Deep Dive — How Spark Actually Executes a Job

**Scenario:** Your Spark revenue aggregation ran in 3 minutes last week but now takes 35. A senior engineer asks for evidence before increasing cluster size.

## 1. From Python to execution

A DataFrame transformation creates a **logical plan**, not immediate computation. An **action** (count, collect, save) triggers a job. Spark's optimizer can push filters closer to the source and change join strategies. Physical execution is split into stages separated by exchanges/shuffles; each stage has tasks working on input partitions.

~~~python
from pyspark.sql import functions as F
sales = spark.read.parquet("output/silver")
paid = sales.filter(F.col("status") == "paid")
by_category = paid.groupBy("category").agg(F.sum("quantity").alias("units"))
by_category.explain("formatted")   # inspect plan, does not run the job
by_category.write.mode("overwrite").parquet("output/category_units")
~~~

Compare narrow transformations (filter/map, same partition dependency) with wide transformations (groupBy/join/repartition, redistribution among executors).

## 2. Why shuffle becomes expensive

Each executor has to serialize, write, transfer, sort/hash and read shuffled records. If one category represents 90% of transactions, one reducer might get almost all records (skew). Doubling executors may not accelerate the straggler.

For a small dimension, broadcasting can avoid shuffling the large fact:

~~~python
dimensions = spark.read.parquet("dimensions/products")
joined = sales.join(F.broadcast(dimensions), on="product_id")
joined.explain("formatted")
~~~

Do not broadcast a dimension too large for worker memory. The fact that Spark offers a hint does not make it a universal optimization.

## 3. Partitioning is a physical design choice

Partitioning by business date helps queries that filter by date; partitioning by order_id often creates many directories and tiny files. Repartition before writing is not always equivalent to persistent table partitioning. Use file counts, size distribution, scan statistics and query duration to decide.

## 4. Work ticket: SLA incident

- [ ] Record dataset size, distinct join keys, skew distribution and hardware limits.
- [ ] Capture physical plans with explain().
- [ ] Measure count or write job runtime *after warm-up*, controlling for cache.
- [ ] Note shuffle read/write, number of tasks, failed tasks and max task duration in Spark UI.
- [ ] Try one controlled change (filter pushdown, broadcast, repartition or skew mitigation).
- [ ] Compare before/after and write an ADR: benefit, regressions, memory risks.

## 5. Failure drill

Create a synthetic data skew by assigning 90% of records the same product category. Does runtime track CPU count, network exchange, serialization, or one long task? State which metric distinguishes the hypotheses.

## Interview

**What is the difference between a Spark job, stage and task?** Describe a groupBy from source rows to final partitioned output without referencing only API names.

Sources: https://spark.apache.org/docs/latest/sql-performance-tuning.html and https://spark.apache.org/docs/latest/rdd-programming-guide.html
