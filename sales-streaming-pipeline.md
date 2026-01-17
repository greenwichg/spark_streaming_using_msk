# Real-Time Product Sales Data Pipeline for Electronic Devices

**Project Title:** Real-Time Sales and Inventory Pipeline for Laptops and Devices.

**Objective:** The goal of this project is to build a real-time data processing pipeline for a retail company that sells laptops and electronic devices. The system will process sales data in real-time, perform aggregations to calculate the total sales by product and brand, and update inventory levels. The results will be pushed to Kafka for real-time monitoring and stored in Parquet format for analysis.

## Scenario:
You are working for an e-commerce company that specializes in selling laptops and other electronic devices. The company wants to track real-time sales data, aggregate it by product category (e.g., laptops, smartphones, tablets) and brand (e.g., Dell, Apple, Lenovo), and monitor inventory levels to prevent stockouts. The data pipeline should also store historical sales data for analytical reporting.

## Technology Stack:
1. **Database:** Any relational database (e.g., PostgreSQL, MySQL)
2. **Spark:** PySpark for real-time data processing
3. **Kafka:** For streaming real-time product sales data
4. **Parquet:** For storing historical processed data
5. **HDFS or S3** for long-term Parquet storage

## Data Flow:
1. **Source:** Sales transactions table in the relational database.
2. **Streaming:** Data is streamed from Kafka to Spark.
3. **Aggregation:** Spark performs real-time aggregation on product sales (total sales and revenue by product and brand).
4. **Sink:** Aggregated results are pushed back to Kafka for real-time monitoring and stored as Parquet files for future analysis.

## Project Description:

### Step 1: Database Setup
The product sales transactions are stored in a table with the following structure:
The table captures real-time sales transactions for different products.

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | INT | Unique ID for each transaction |
| product_name | VARCHAR | Name of the product (e.g., Dell Inspiron, Apple iPhone) |
| product_category | VARCHAR | Category of the product (e.g., laptop, smartphone) |
| Brand | VARCHAR | Brand of the product (e.g., Dell, Apple, Lenovo) |
| quantity_sold | INT | Number of units sold |
| price_per_unit | DOUBLE | Price per unit of the product |
| transaction_date | TIMESTAMP | Timestamp of the transaction |

### Step 2: Kafka Producer (Stream Source)
A Kafka producer pushes each product sales transaction from the database into a Kafka topic (product_sales).

Kafka producer to stream data from the database:
```python
from confluent_kafka import Producer
import mysql.connector

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(**conf)

# Connect to the database
conn = mysql.connector.connect(host='localhost', database='ecommerce', user='root', password='password')

# Query the data
cursor = conn.cursor()
cursor.execute("SELECT * FROM product_sales")

# Produce messages to Kafka topic
for row in cursor:
    message = {
        'transaction_id': row[0],
        'product_name': row[1],
        'product_category': row[2],
        'brand': row[3],
        'quantity_sold': row[4],
        'price_per_unit': row[5],
        'transaction_date': row[6].strftime('%Y-%m-%d %H:%M:%S')
    }
    producer.produce('product_sales', key=str(row[0]), value=str(message))

# Close connections
cursor.close()
conn.close()
producer.flush()
```

### Step 3: Spark Streaming to Consume Kafka Messages
Use Spark Structured Streaming to read Kafka messages, perform aggregations, and output the results to a new Kafka topic and to Parquet files.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("ProductSalesAggregation").getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("quantity_sold", IntegerType(), True),
    StructField("price_per_unit", DoubleType(), True),
    StructField("transaction_date", TimestampType(), True)
])

# Read Kafka Stream
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "product_sales") \
    .load()

# Convert binary value to string
sales_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse JSON and extract the fields
parsed_df = sales_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Perform aggregation: Total quantity and revenue per product and brand
aggregated_df = parsed_df.groupBy("product_name", "brand", "product_category").agg(
    sum("quantity_sold").alias("total_quantity_sold"),
    expr("sum(quantity_sold * price_per_unit)").alias("total_revenue")
)

# Write aggregated data to Kafka
query_kafka = aggregated_df.selectExpr("CAST(product_name AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregated_product_sales") \
    .option("checkpointLocation", "/tmp/checkpoints/product_sales_kafka") \
    .outputMode("complete") \
    .start()

# Write aggregated data to Parquet files
query_parquet = aggregated_df.writeStream \
    .format("parquet") \
    .option("path", "/path/to/output/parquet") \
    .option("checkpointLocation", "/tmp/checkpoints/product_sales_parquet") \
    .outputMode("append") \
    .start()

# Wait for queries to terminate
query_kafka.awaitTermination()
query_parquet.awaitTermination()
```

### Step 4: Output to Kafka and Parquet
1. The aggregated data (total quantity sold and total revenue by product and brand) is sent back to a Kafka topic (aggregated_product_sales) for real-time monitoring.
2. At the same time, the data is stored in Parquet format for future analysis and reporting.

This real-time pipeline provides the following key features:
1. **Real-time sales monitoring:** Aggregated sales and revenue data for each product and brand are available instantly via Kafka.
2. **Scalable architecture:** Spark and Kafka allow for scalable processing of large datasets with low latency.
3. **Historical data storage:** All sales data is saved as Parquet files, enabling analysis and reporting of historical trends.

## Example Product Sales Data:
Assume we have the following product sales transactions:
- **Transaction ID:** 3001, **Product:** Dell Inspiron, **Category:** Laptop, **Brand:** Dell, **Quantity Sold:** 2, **Price Per Unit:** ₹50000
- **Transaction ID:** 3002, **Product:** Apple iPhone 12, **Category:** Smartphone, **Brand:** Apple, **Quantity Sold:** 3, **Price Per Unit:** ₹80000
- **Transaction ID:** 3003, **Product:** Lenovo ThinkPad, **Category:** Laptop, **Brand:** Lenovo, **Quantity Sold:** 1, **Price Per Unit:** ₹65000

Aggregated results would look like:
- **Dell Inspiron (Laptop):** Total Quantity Sold: 2, Total Revenue: ₹100000
- **Apple iPhone 12 (Smartphone):** Total Quantity Sold: 3, Total Revenue: ₹240000
- **Lenovo ThinkPad (Laptop):** Total Quantity Sold: 1, Total Revenue: ₹65000

## Next Steps to Add in Future Improvements
1. **Window-based aggregations:** Implement windowing to track product sales over time, such as daily or hourly sales trends. This would provide insights into high-sales periods and allow for more dynamic inventory management.
2. **Inventory management:** Extend the pipeline by integrating it with real-time inventory data. Add alerts when stock levels reach a predefined threshold, helping to avoid stockouts and ensure products are always available for customers.
3. **Customer segmentation:** Track which customer segments are buying certain brands or products (e.g., high-end laptops) and use the data for targeted marketing campaigns.
4. **Predictive analytics:** Incorporate machine learning models that predict sales demand based on historical data. Predictive models can help the company better manage inventory, reducing both excess stock and the risk of stockouts.
5. **Personalized recommendations:** Use the sales data to provide personalized product recommendations for customers, based on their purchase history and popular products in their geographic area.

## Step 5: Future Improvements with Implementation

### 1. Window-based Aggregations
**Objective:** Implement time-based windowing to aggregate sales data over daily or hourly intervals to gain insights into sales trends.

Using Spark Structured Streaming, implement window-based aggregations using the window function. This function allows grouping data into time windows.

```python
from pyspark.sql.functions import window

# Group data by product_name, brand, and time window (e.g., 1-hour window)
windowed_aggregation = parsed_df.groupBy(
    window(col("transaction_date"), "1 hour"),  # 1-hour time window
    col("product_name"),
    col("brand")
).agg(
    sum("quantity_sold").alias("total_quantity_sold"),
    expr("sum(quantity_sold * price_per_unit)").alias("total_revenue")
)

# Write windowed aggregation to Parquet
windowed_aggregation.writeStream \
    .format("parquet") \
    .option("path", "/path/to/output/windowed_sales_parquet") \
    .option("checkpointLocation", "/tmp/checkpoints/windowed_sales_parquet") \
    .outputMode("append") \
    .start()

# Write windowed aggregation to Kafka
windowed_aggregation.selectExpr("CAST(product_name AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "windowed_product_sales") \
    .option("checkpointLocation", "/tmp/checkpoints/windowed_sales_kafka") \
    .outputMode("complete") \
    .start()
```

**Output:** The data is now aggregated on an hourly basis for each product and brand, and both the aggregated results and sales trends over time are stored in Kafka and Parquet files.

### 2. Inventory Management with Stock Alerts
**Objective:** Extend the pipeline to integrate inventory management and send alerts when stock levels fall below a threshold.

1. **Inventory Table:** Add an inventory table that tracks the number of units in stock.
```sql
CREATE TABLE inventory (
    product_name VARCHAR(255),
    brand VARCHAR(255),
    stock_level INT
);
```

2. **Inventory Tracking in Spark:** Join sales data with inventory data to track stock levels.
```python
# Load inventory data from the database
inventory_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/ecommerce") \
    .option("dbtable", "inventory") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

# Join sales data with inventory data
inventory_tracking_df = aggregated_df.join(inventory_df, ["product_name", "brand"], "inner") \
    .withColumn("updated_stock", col("stock_level") - col("total_quantity_sold"))

# Filter out products where stock is below a threshold
low_stock_df = inventory_tracking_df.filter(col("updated_stock") < 10)

# Send alerts for products with low stock levels (Kafka or Email)
low_stock_df.selectExpr("CAST(product_name AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "low_stock_alerts") \
    .option("checkpointLocation", "/tmp/checkpoints/low_stock") \
    .start()
```

**Output:** When the stock level for any product drops below a certain threshold (e.g., 10 units), the system sends real-time alerts via Kafka.

### 3. Customer Segmentation for Targeted Marketing
**Objective:** Track customer segments purchasing specific products or brands and use the data for targeted marketing.

Assume you have a customer_data table that stores customer information.
```sql
CREATE TABLE customer_data (
    customer_id INT,
    customer_name VARCHAR(255),
    customer_segment VARCHAR(255)  -- Segments like "Premium", "Budget"
);
```

You can join the sales data with customer data to track which segments are buying which products.
```python
# Load customer data from the database
customer_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/ecommerce") \
    .option("dbtable", "customer_data") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

# Join sales data with customer data
customer_segmentation_df = parsed_df.join(customer_df, "customer_id")

# Aggregate sales by customer segment and product
segment_aggregation_df = customer_segmentation_df.groupBy("customer_segment", "product_name", "brand").agg(
    sum("quantity_sold").alias("total_quantity_sold"),
    expr("sum(quantity_sold * price_per_unit)").alias("total_revenue")
)

# Write the results to Kafka or Parquet
segment_aggregation_df.selectExpr("CAST(customer_segment AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "customer_segment_sales") \
    .option("checkpointLocation", "/tmp/checkpoints/customer_segment_sales") \
    .start()
```

**Output:** This implementation tracks customer segments buying specific products and allows for targeted marketing campaigns based on the data.

### 4. Predictive Analytics for Sales Demand Forecasting
**Objective:** Use historical sales data and machine learning models to predict future sales demand.

1. **Historical Data:** Ensure you have enough historical data saved in Parquet files.
2. **Model Training:** Use a machine learning library like Spark MLlib to train a model on the historical sales data. For example, use a Linear Regression model to predict future demand based on previous sales trends.

```python
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Load historical sales data (assumed to be stored in Parquet)
historical_sales_df = spark.read.parquet("/path/to/historical_sales_parquet")

# Feature engineering
assembler = VectorAssembler(inputCols=["total_quantity_sold", "total_revenue"], outputCol="features")
training_data = assembler.transform(historical_sales_df)

# Train a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="total_quantity_sold")
lr_model = lr.fit(training_data)

# Predict future sales using the trained model
predictions = lr_model.transform(training_data)

# Write the predictions back to Kafka or save to Parquet
predictions.select("product_name", "prediction").writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "sales_predictions") \
    .option("checkpointLocation", "/tmp/checkpoints/sales_predictions") \
    .start()
```

**Output:** The system can predict future demand for each product and output the predictions to Kafka for monitoring or to Parquet for further analysis.

### 5. Personalized Product Recommendations
**Objective:** Recommend products to customers based on their past purchase history and popular products in their region.

You can build a simple recommendation system using collaborative filtering in Spark MLlib.

```python
from pyspark.ml.recommendation import ALS
# Load customer purchase history
purchase_history_df = spark.read.parquet("/path/to/purchase_history")

# Train the ALS recommendation model
als = ALS(userCol="customer_id", itemCol="product_id", ratingCol="quantity_sold", coldStartStrategy="drop")
als_model = als.fit(purchase_history_df)

# Generate top 5 product recommendations for each customer
recommendations = als_model.recommendForAllUsers(5)

# Write recommendations to Kafka
recommendations.selectExpr("CAST(customer_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "product_recommendations") \
    .option("checkpointLocation", "/tmp/checkpoints/product_recommendations") \
    .start()
```

**Output:** This implementation recommends products to customers based on past purchase history and popular products among similar users, with real-time updates sent to Kafka.