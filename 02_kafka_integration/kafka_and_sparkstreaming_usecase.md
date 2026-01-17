# Kafka and Spark Streaming Use Case

This exercise covers a example of streaming data using Kafka and processing it with Spark Structured Streaming using:
1. Max byte size and max number of messages per batch.
2. Sliding windows for real-time data processing.
3. Aggregations such as avg, max, and min values.

This example and data is to simulate a sensor system where multiple sensors stream data to Kafka, and we use Spark to process that data in real time.

## Step 1: Sample Data
Take this data and push into a Kafka topic to use with Spark Streaming. (Assume that this data is keep on coming from source.)

```json
[
    {"sensor_id": "sensor_001", "value": 23.5, "timestamp": "2024-10-22T10:40:00"},
    {"sensor_id": "sensor_002", "value": 26.7, "timestamp": "2024-10-22T10:41:00"},
    {"sensor_id": "sensor_003", "value": 34.2, "timestamp": "2024-10-22T10:42:00"},
    {"sensor_id": "sensor_004", "value": 29.9, "timestamp": "2024-10-22T10:43:00"},
    {"sensor_id": "sensor_005", "value": 38.4, "timestamp": "2024-10-22T10:44:00"},
    {"sensor_id": "sensor_006", "value": 50.6, "timestamp": "2024-10-22T10:45:00"},
    {"sensor_id": "sensor_007", "value": 42.1, "timestamp": "2024-10-22T10:46:00"},
    {"sensor_id": "sensor_008", "value": 44.8, "timestamp": "2024-10-22T10:47:00"},
    {"sensor_id": "sensor_001", "value": 46.0, "timestamp": "2024-10-22T10:48:00"},
    {"sensor_id": "sensor_002", "value": 49.3, "timestamp": "2024-10-22T10:49:00"},
    {"sensor_id": "sensor_003", "value": 32.5, "timestamp": "2024-10-22T10:50:00"},
    {"sensor_id": "sensor_004", "value": 36.7, "timestamp": "2024-10-22T10:51:00"}
]
```

Here, each record represents:
- **sensor_id:** ID of the sensor (e.g., sensor_001).
- **value:** The measured value (temperature, humidity, etc.).
- **timestamp:** The timestamp when the data was captured.

## Step 2: Kafka Producer (Simulate Data Streaming)
The Kafka producer will stream the above data into a Kafka topic called sensor-data. The producer sends records to Kafka every half-second.

```python
from kafka import KafkaProducer
import json
from time import sleep

# Kafka producer setup
producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Sample sensor records
sensor_data = [
    {"sensor_id": "sensor_001", "value": 23.5, "timestamp": "2024-10-22T10:40:00"},
    {"sensor_id": "sensor_002", "value": 26.7, "timestamp": "2024-10-22T10:41:00"},
    {"sensor_id": "sensor_003", "value": 34.2, "timestamp": "2024-10-22T10:42:00"},
    {"sensor_id": "sensor_004", "value": 29.9, "timestamp": "2024-10-22T10:43:00"},
    {"sensor_id": "sensor_005", "value": 38.4, "timestamp": "2024-10-22T10:44:00"},
    {"sensor_id": "sensor_006", "value": 50.6, "timestamp": "2024-10-22T10:45:00"},
    {"sensor_id": "sensor_007", "value": 42.1, "timestamp": "2024-10-22T10:46:00"},
    {"sensor_id": "sensor_008", "value": 44.8, "timestamp": "2024-10-22T10:47:00"},
    {"sensor_id": "sensor_001", "value": 46.0, "timestamp": "2024-10-22T10:48:00"},
    {"sensor_id": "sensor_002", "value": 49.3, "timestamp": "2024-10-22T10:49:00"},
    {"sensor_id": "sensor_003", "value": 32.5, "timestamp": "2024-10-22T10:50:00"},
    {"sensor_id": "sensor_004", "value": 36.7, "timestamp": "2024-10-22T10:51:00"}
]

# Stream data to Kafka topic 'sensor-data'
for record in sensor_data:
    producer.send('sensor-data', record)
    print(f"Produced: {record}")
    sleep(0.5)  # Simulate streaming delay
```

## Step 3: Spark Streaming with Kafka - Sliding Window, Byte/Message Limits, and Aggregations
Now, use Spark Structured Streaming with:
1. Limit the max number of messages and bytes per micro-batch.
2. Apply a sliding window of 10 minutes with a 5-minute slide.
3. Perform aggregations (average, max, and min values).

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaMicrobatchProcessingWithSlidingWindow") \
    .getOrCreate()

# Define schema for sensor data
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("value", DoubleType()) \
    .add("timestamp", TimestampType())

# Read from Kafka topic 'sensor-data' with max byte and max record limit
sensor_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .option("maxOffsetsPerTrigger", 5)  # Limit the number of messages per batch
    .option("fetchMaxBytes", 1024 * 10)  # Limit the batch size to 10 KB
    .load()

# Convert Kafka data (binary) to JSON and apply schema
sensor_data = sensor_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Sliding window of 10 minutes with a slide duration of 5 minutes
windowed_data = sensor_data \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),  # Sliding window
        col("sensor_id") ) \
    .agg(
        {"value": "avg", "value": "max", "value": "min"}  # Aggregations: avg, max, min) \
    .withColumnRenamed("avg(value)", "avg_value") \
    .withColumnRenamed("max(value)", "max_value") \
    .withColumnRenamed("min(value)", "min_value")

# Write the output to the console
query = windowed_data \
    .writeStream \
    .outputMode("update") \
    .option("truncate", "false") \
    .format("console") \
    .start()

# Await termination
query.awaitTermination()
```

## Explanation of Key Components:
1. **Kafka Source Options:**
   - `option("maxOffsetsPerTrigger", 5)`: Limits the number of messages processed in each micro-batch to 5 messages.
   - `option("fetchMaxBytes", 1024 * 10)`: Limits the amount of data (in bytes) to read in a single micro-batch to 10 KB.
2. **Sliding Window:**
   - `window(col("timestamp"), "10 minutes", "5 minutes")`: Defines a sliding window of 10 minutes, with a slide interval of 5 minutes. This means a new window is triggered every 5 minutes, but it looks back at data from the last 10 minutes.
3. **Aggregations:**
   - We perform multiple aggregations: `avg(value)` (average), `max(value)` (maximum), and `min(value)` (minimum) for each sensor in each sliding window.
4. **Watermarking:**
   - `.withWatermark("timestamp", "5 minutes")`: A watermark ensures late data older than 5 minutes will be ignored, helping with state management in streaming queries.