# Spark Streaming with Tumbling, Sliding, and Session Windows

A use case for Spark Streaming that uses all three types of windows: Tumbling, Sliding, and Session Windows, and also handles late-arriving data.

Here, we will simulate a stream of users being processed as if they represent events such as "user activity" (like logging in or performing an action), where each event has a timestamp.

## Use Case:
Simulate a system where users are performing activities, and we want to:
- Track the number of activities per tumbling window (fixed interval).
- Track active users every sliding window (sliding interval).
- Group activities by users' session time (activity clustering with inactivity gaps).
- Handle late-arriving data using watermarking.

## Data:
Each record will consist of the following fields:
1. **name (string)** — Name of the user.
2. **timestamp (timestamp)** — Event time of the user's activity.
3. **activity (string)** — Type of activity (e.g., "login", "action").

### Example Data:

| name | timestamp | activity |
|------|-----------|----------|
| Ravi | 2024-10-23 10:05:00 | login |
| Priya | 2024-10-23 10:06:00 | action |
| Rakesh | 2024-10-23 10:07:00 | action |
| Anjali | 2024-10-23 10:08:00 | login |
| Ravi | 2024-10-23 10:12:00 | action |
| Priya | 2024-10-23 10:13:00 | action |

## Requirements:
- **Tumbling window** to count the number of activities per 5-minute interval.
- **Sliding window** to track the unique users performing activities in the last 10 minutes, sliding every 2 minutes.
- **Session window** to track user sessions (i.e., activities that are within 5 minutes of each other are grouped into one session).
- **Handle late-arriving data** up to 2 minutes.

## Spark Streaming Code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, session_window, count, approx_count_distinct, watermark
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("SparkStreamingWindows").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")

# Define schema for the input data
schema = StructType([
StructField("name", StringType(), True),
StructField("timestamp", TimestampType(), True),
StructField("activity", StringType(), True)
])

# Simulated input data (replace with actual streaming source like Kafka)
data = [
    ("Ravi", "2024-10-23 10:05:00", "login"),
    ("Priya", "2024-10-23 10:06:00", "action"),
    ("Rakesh", "2024-10-23 10:07:00", "action"),
    ("Anjali", "2024-10-23 10:08:00", "login"),
    ("Ravi", "2024-10-23 10:12:00", "action"),
    ("Priya", "2024-10-23 10:13:00", "action")
]

# Create DataFrame from static data (in a real scenario, replace with readStream)
df = spark.createDataFrame(data, schema=schema)

# Apply watermarking to handle late-arriving data
df_with_watermark = df.withWatermark("timestamp", "2 minutes")

# Tumbling Window (5-minute interval)
tumbling_window_df = df_with_watermark.groupBy(
    window(col("timestamp"), "5 minutes")
).count()

# Sliding Window (10-minute window sliding every 2 minutes)
sliding_window_df = df_with_watermark.groupBy(window(col("timestamp"), "10 minutes", "2 minutes"))
.agg(approx_count_distinct("name").alias("unique_users"))

# Session Window (5-minute session timeout)
session_window_df = df_with_watermark.groupBy(
session_window(col("timestamp"), "5 minutes"), col("name")
).count()

# Display results
print("Tumbling Window (5-minute interval):")
tumbling_window_df.show(truncate=False)

print("Sliding Window (10-minute window, 2-minute slide):")
sliding_window_df.show(truncate=False)

print("Session Window (5-minute session timeout):")
session_window_df.show(truncate=False)


# For actual streaming, use `writeStream` instead of `show` for continuous output
# Example:
query = tumbling_window_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
```

## Points to note:
1. **Tumbling Window (Fixed Interval):**
   - Groups data into 5-minute fixed windows, counting the number of events within each window.
2. **Sliding Window (Sliding Interval):**
   - Tracks the number of distinct users in the last 10 minutes, sliding every 2 minutes.
3. **Session Window:**
   - Tracks user sessions, where all activities within 5 minutes of each other are grouped into the same session.
4. **Watermarking:**
   - Ensures that late data (up to 2 minutes late) can still be processed. Events arriving later than 2 minutes past the event time will be dropped.

## Handling Late Data:
By adding watermarking using `.withWatermark("timestamp", "2 minutes")`, the system can handle late-arriving events. The watermark ensures that events arriving within 2 minutes of the event time are still processed, but events arriving later are discarded.

## Output:
- The **Tumbling Window** will output the count of events per 5-minute window.
- The **Sliding Window** will show how many unique users were active in a sliding 10-minute window.
- The **Session Window** will cluster user activities into sessions, with a 5-minute session timeout.