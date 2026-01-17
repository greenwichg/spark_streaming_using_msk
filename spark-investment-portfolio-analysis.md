# Project Title: Investment Portfolio Analysis

## Project Overview

**Objective:** Analyze investment portfolios for clients, including transaction history, client details, and investment performance, using Spark transformations. The project provides insights into investment patterns, transaction summaries, and risk profiling, demonstrating practical applications of Spark transformations.

**Tools:** Apache Spark, PySpark

**Data Source:** JSON dataset representing clients, investments, and transactions.

## Input Data

### Clients.json
```json
[
  {"ClientID": "C001", "Name": "Ravi Kumar", "Age": 45, "City": "Mumbai", "RiskProfile": "Moderate"},
  {"ClientID": "C002", "Name": "Priya Sharma", "Age": 30, "City": "Delhi", "RiskProfile": "High"},
  {"ClientID": "C003", "Name": "Aman Verma", "Age": 50, "City": "Bangalore", "RiskProfile": "Low"},
  {"ClientID": "C004", "Name": "Neha Srivastava", "Age": 35, "City": "Kolkata", "RiskProfile": "Moderate"}
]
```

### Investments.json
```json
[
  {"InvestmentID": "I001", "ClientID": "C001", "Type": "Mutual Fund", "Amount": 500000, "ROI": 12},
  {"InvestmentID": "I002", "ClientID": "C002", "Type": "Stocks", "Amount": 300000, "ROI": 20},
  {"InvestmentID": "I003", "ClientID": "C003", "Type": "Bonds", "Amount": 200000, "ROI": 8},
  {"InvestmentID": "I004", "ClientID": "C004", "Type": "Real Estate", "Amount": 1000000, "ROI": 10}
]
```

### Transactions.json
```json
[
  {"TransactionID": "T001", "ClientID": "C001", "InvestmentID": "I001", "Amount": 10000, "Type": "Deposit"},
  {"TransactionID": "T002", "ClientID": "C002", "InvestmentID": "I002", "Amount": 5000, "Type": "Withdrawal"},
  {"TransactionID": "T003", "ClientID": "C003", "InvestmentID": "I003", "Amount": 8000, "Type": "Deposit"},
  {"TransactionID": "T004", "ClientID": "C004", "InvestmentID": "I004", "Amount": 20000, "Type": "Deposit"}
]
```

## Use Cases and Transformations

### 1. window()
**Objective:** Calculate the running total of transaction amounts for each client.

```python
from pyspark.sql import Window
from pyspark.sql.functions import sum

window_spec = Window.partitionBy("ClientID").orderBy("TransactionID")
running_total = transactions_df.withColumn("RunningTotal", sum("Amount").over(window_spec))
running_total.show()
```

### 2. agg()
**Objective:** Calculate the total and average investment amounts for all clients.

```python
from pyspark.sql.functions import avg, sum

investment_summary = investments_df.agg(
    sum("Amount").alias("TotalInvestment"),
    avg("Amount").alias("AverageInvestment")
)
investment_summary.show()
```

### 3. groupByKeyAndCount()
**Objective:** Count the number of investments by each type.

```python
investment_counts = investments_df.groupBy("Type").count()
investment_counts.show()
```

### 4. mapPartitionsWithKey()
**Objective:** Apply a custom logic to process transaction amounts by partition.

```python
def partition_processor(partition):
    for record in partition:
        yield (record["ClientID"], record["Amount"] * 1.1)

processed_rdd = transactions_rdd.mapPartitionsWithKey(partition_processor)
processed_rdd.collect()
```

### 5. selectExpr()
**Objective:** Use SQL expressions to create a new column for ROI percentage.

```python
investments_df.selectExpr("Type", "ROI * 100 as ROI_Percentage").show()
```

### 6. repartition()
**Objective:** Repartition the investments DataFrame for optimization.

```python
repartitioned_df = investments_df.repartition(4)
print(repartitioned_df.rdd.getNumPartitions())
```

### 7. replace()
**Objective:** Replace "High" risk profile with "Very High".

```python
updated_clients_df = clients_df.replace({"High": "Very High"}, subset=["RiskProfile"])
updated_clients_df.show()
```

### 8. distinct()
**Objective:** Find distinct cities from the clients dataset.

```python
distinct_cities = clients_df.select("City").distinct()
distinct_cities.show()
```

### 9. dropDuplicates()
**Objective:** Remove duplicate transactions.

```python
unique_transactions = transactions_df.dropDuplicates()
unique_transactions.show()
```

### 10. describe()
**Objective:** Compute summary statistics for investment amounts.

```python
investments_df.describe("Amount").show()
```

### 11. transform()
**Objective:** Apply a custom transformation to normalize ROI.

```python
def normalize_roi(df):
    from pyspark.sql.functions import col
    return df.withColumn("NormalizedROI", col("ROI") / 100)

normalized_df = investments_df.transform(normalize_roi)
normalized_df.show()
```

### 12. insertInto()
**Objective:** Insert processed data into a Hive table.

```python
normalized_df.write.insertInto("hive_table_name")
```

### 13. cache()
**Objective:** Cache the investments DataFrame for reuse.

```python
investments_df.cache()
investments_df.count()
```

### 14. broadcast()
**Objective:** Broadcast the clients DataFrame for joins.

```python
from pyspark.sql.functions import broadcast

joined_df = investments_df.join(broadcast(clients_df), "ClientID")
joined_df.show()
```

### 15. createOrReplaceTempView()
**Objective:** Create a temporary view for SQL queries.

```python
investments_df.createOrReplaceTempView("investments")
spark.sql("SELECT * FROM investments WHERE ROI > 10").show()
```

### 16. createOrReplaceGlobalTempView()
**Objective:** Create a global temporary view.

```python
investments_df.createOrReplaceGlobalTempView("global_investments")
spark.sql("SELECT * FROM global_temp.global_investments").show()
```

### 17. explain()
**Objective:** View the execution plan of a query.

```python
investments_df.select("Amount").explain()
```

### 18. write()
**Objective:** Write the DataFrame to a Parquet file.

```python
investments_df.write.parquet("/path/to/output.parquet")
```

### 19. merge()
**Objective:** Merge clients and transactions data.

```python
merged_df = clients_df.join(transactions_df, "ClientID", "inner")
merged_df.show()
```

### 20. fillna()
**Objective:** Fill null values in transaction amounts with 0.

```python
transactions_df.fillna(0, subset=["Amount"]).show()
```

### 21. transform()
**Objective:** Add a "HighValue" column for investments over 5 lakhs.

```python
def add_high_value_column(df):
    from pyspark.sql.functions import when, col
    return df.withColumn("HighValue", when(col("Amount") > 500000, True).otherwise(False))

high_value_df = investments_df.transform(add_high_value_column)
high_value_df.show()
```

### 22. unionByName()
**Objective:** Combine two DataFrames with the same schema.

```python
combined_df = df1.unionByName(df2)
combined_df.show()
```

### 23. sample()
**Objective:** Randomly sample 50% of the transactions.

```python
sampled_df = transactions_df.sample(0.5)
sampled_df.show()
```

### 24. drop()
**Objective:** Drop the "City" column from clients DataFrame.

```python
clients_df.drop("City").show()
```

### 25. intersect()
**Objective:** Find common clients between two DataFrames.

```python
common_clients = df1.intersect(df2)
common_clients.show()
```

### 26. limit()
**Objective:** Limit the output to the first 5 investments.

```python
investments_df.limit(5).show()
```

### 27. rename()
**Objective:** Rename the "Amount" column to "InvestmentAmount".

```python
renamed_df = investments_df.withColumnRenamed("Amount", "InvestmentAmount")
renamed_df.show()
```

### 47. where()
**Objective:** Filter investments with ROI greater than 10.

```python
filtered_df = investments_df.where("ROI > 10")
filtered_df.show()
```

### 48. dropna()
**Objective:** Drop rows with null values in the transactions DataFrame.

```python
cleaned_df = transactions_df.dropna()
cleaned_df.show()
```