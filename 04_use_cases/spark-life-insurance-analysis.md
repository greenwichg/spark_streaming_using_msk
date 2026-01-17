# Project Title: Life Insurance Data Analysis

## Project Overview
Analyze life insurance data to derive insights on policies, claims, and customer behavior using Spark transformations. The project will involve multiple Spark transformations to process and analyze the data effectively, showcasing real-world scenarios where these transformations are applied.

**Tools:** Apache Spark, PySpark

**Data Source:** Simulated dataset representing life insurance policies, customers, and claims.

## Scenario Description:
A life insurance company wants to analyze its policy and claims data to:
1. Group policies by agents.
2. Aggregate claim amounts per policy type.
3. Perform various joins to combine datasets.
4. Identify unique agents and customers.
5. Restructure and sort the data for reporting.

The dataset includes:
- **Policies:** Details of policies issued.
- **Claims:** Claims filed by customers.
- **Customers:** Information about policyholders.

## Input Data:

### 1. Policies.csv
```
PolicyID,PolicyType,AgentName,PremiumAmount,CustomerID
P001,Term,Rajesh Gupta,12000,C101
P002,Endowment,Sunita Mehta,15000,C102
P003,Whole Life,Rajesh Gupta,20000,C103
P004,Term,Sunita Mehta,10000,C104
```

### 2. Claims.csv
```
ClaimID,PolicyID,ClaimAmount
CL001,P001,5000
CL002,P002,15000
CL003,P003,20000
CL004,P004,10000
```

### 3. Customers.csv
```
CustomerID,Name,Age,City
C101,Ravi Kumar,45,Mumbai
C102,Priya Sharma,30,Delhi
C103,Aman Verma,50,Bangalore
C104,Neha Srivastava,35,Kolkata
```

## Transformations and Code:

### 1. groupByKey()
**Objective:** Group policy premiums by agents.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("LifeInsuranceAnalysis").getOrCreate()
policies = [("P001", "Term", "Rajesh Gupta", 12000, "C101"),
            ("P002", "Endowment", "Sunita Mehta", 15000, "C102"),
            ("P003", "Whole Life", "Rajesh Gupta", 20000, "C103"),
            ("P004", "Term", "Sunita Mehta", 10000, "C104")]

policies_rdd = spark.sparkContext.parallelize(policies)
agent_premiums = policies_rdd.map(lambda x: (x[2], x[3])).groupByKey()
result = agent_premiums.mapValues(list).collect()
print(result)
```

### 2. reduceByKey()
**Objective:** Calculate total premiums per agent.

```python
agent_total_premiums = policies_rdd.map(lambda x: (x[2], x[3])).reduceByKey(lambda x, y: x + y).collect()
print(agent_total_premiums)
```

### 3. join()
**Objective:** Combine policy and claim data.

```python
claims = [("CL001", "P001", 5000),
          ("CL002", "P002", 15000),
          ("CL003", "P003", 20000),
          ("CL004", "P004", 10000)]
claims_rdd = spark.sparkContext.parallelize(claims)
policy_claims = policies_rdd.map(lambda x: (x[0], x)).join(claims_rdd.map(lambda x: (x[1], x))).collect()
print(policy_claims)
```

### 4. leftOuterJoin()
**Objective:** Ensure all policies are included, even if no claims exist.

```python
policy_claims_left = policies_rdd.map(lambda x: (x[0], x)).leftOuterJoin(claims_rdd.map(lambda x: (x[1], x))).collect()
print(policy_claims_left)
```

### 5. rightOuterJoin()
**Objective:** Ensure all claims are included, even if no matching policy exists.

```python
policy_claims_right = policies_rdd.map(lambda x: (x[0], x)).rightOuterJoin(claims_rdd.map(lambda x: (x[1], x))).collect()
print(policy_claims_right)
```

### 6. fullOuterJoin()
**Objective:** Combine policies and claims, including unmatched entries.

```python
policy_claims_full = policies_rdd.map(lambda x: (x[0], x)).fullOuterJoin(claims_rdd.map(lambda x: (x[1], x))).collect()
print(policy_claims_full)
```

### 7. sortBy()
**Objective:** Sort policies by premium amount.

```python
sorted_policies = policies_rdd.sortBy(lambda x: x[3]).collect()
print(sorted_policies)
```

### 8. aggregateByKey()
**Objective:** Find the maximum and total premium per agent.

```python
aggregate_result = policies_rdd.map(lambda x: (x[2], x[3])).aggregateByKey((0, 0),
    lambda acc, value: (max(acc[0], value), acc[1] + value),
    lambda acc1, acc2: (max(acc1[0], acc2[0]), acc1[1] + acc2[1])
).collect()
print(aggregate_result)
```

### 9. cogroup()
**Objective:** Group policies and claims by PolicyID.

```python
policy_rdd = policies_rdd.map(lambda x: (x[0], x))
claim_rdd = claims_rdd.map(lambda x: (x[1], x))
cogrouped_data = policy_rdd.cogroup(claim_rdd).mapValues(lambda x: (list(x[0]), list(x[1]))).collect()
print(cogrouped_data)
```

### 10. subtract()
**Objective:** Find policies with no associated claims.

```python
policy_ids = policy_rdd.map(lambda x: x[0])
claim_ids = claim_rdd.map(lambda x: x[0])
unclaimed_policies = policy_ids.subtract(claim_ids).collect()
print(unclaimed_policies)
```

### 11. sampleByKey()
**Objective:** Sample a subset of policies by AgentName.

```python
sampled_policies = policies_rdd.map(lambda x: (x[2], x)).sampleByKey(True, {"Rajesh Gupta": 0.5, "Sunita Mehta": 0.5}).collect()
print(sampled_policies)
```

### 12. combineByKey()
**Objective:** Compute average premium per agent.

```python
combine_result = policies_rdd.map(lambda x: (x[2], x[3])).combineByKey(
    lambda value: (value, 1),  # Create combiner
    lambda acc, value: (acc[0] + value, acc[1] + 1),  # Merge value
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # Merge combiners
).mapValues(lambda x: x[0] / x[1]).collect()
print(combine_result)
```

### 13. pivot()
**Objective:** Restructure claims data to display ClaimAmount by PolicyID.

```python
from pyspark.sql import Row
claims_df = spark.createDataFrame([Row(ClaimID=row[0], PolicyID=row[1], ClaimAmount=row[2]) for row in claims])
pivoted_claims = claims_df.groupBy("PolicyID").pivot("ClaimID").sum("ClaimAmount").show()
```

**Additional Example with JSON Data:**
```python
# Sample JSON data
json_data = [
    {"PolicyID": "P001", "Month": "Jan", "ClaimAmount": 5000},
    {"PolicyID": "P001", "Month": "Feb", "ClaimAmount": 3000},
    {"PolicyID": "P002", "Month": "Jan", "ClaimAmount": 15000},
    {"PolicyID": "P003", "Month": "Mar", "ClaimAmount": 20000},
]

# Create DataFrame from JSON
json_df = spark.read.json(spark.sparkContext.parallelize(json_data))
# Pivot the data to show ClaimAmount by Month
pivoted_json = json_df.groupBy("PolicyID").pivot("Month").sum("ClaimAmount").show()
```

### 14. groupBy()
**Objective:** Group policies by PolicyType.

```python
policy_df = spark.createDataFrame([Row(PolicyID=row[0], PolicyType=row[1], AgentName=row[2], PremiumAmount=row[3], CustomerID=row[4]) for row in policies])
grouped_by_policy_type = policy_df.groupBy("PolicyType").count().show()
```

### 15. joinWith()
**Objective:** Join customers and policies with specific structures.

```python
customers_rdd = spark.sparkContext.parallelize(customers)
customers_with_policies = customers_rdd.map(lambda x: (x[0], x)).joinWith(policy_rdd, lambda x: x[1] == x[0]).collect()
print(customers_with_policies)
```

### 16. withColumn()
**Objective:** Add a "Region" column to the customer data based on City.

```python
from pyspark.sql.functions import when
customer_df = spark.createDataFrame([Row(CustomerID=row[0], Name=row[1], Age=row[2], City=row[3]) for row in customers])
customer_with_region = customer_df.withColumn("Region", when(customer_df.City.isin("Mumbai", "Delhi"), "North").otherwise("South")).show()
```

### 17. countByKey()
**Objective:** Count the number of policies per agent.

```python
policy_count_by_agent = policies_rdd.map(lambda x: (x[2], 1)).countByKey()
print(policy_count_by_agent)
```

### 18. crossJoin()
**Objective:** Perform a Cartesian join of policies and claims.

```python
policy_df.crossJoin(claims_df).show()
```

## Expected Output
The project will generate insights such as:
- Total premium collected per agent.
- Claims data linked to policy details.
- Agents with the highest policies sold.
- City-wise distribution of customers.
- Policies without claims.
- Average premium amounts per agent.
- Pivoted claim data for better analysis.
- Grouped data by policy type.