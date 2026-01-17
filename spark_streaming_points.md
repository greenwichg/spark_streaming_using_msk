# Spark Streaming/Structured Streaming

## 1. Accumulators
Accumulators are shared, write-only variables used to perform operations like counters or sums across tasks in parallel computations. They allow for efficient aggregation of data across nodes.

Typically used for debugging (e.g., counting errors) or aggregating statistics across tasks.

**Advantages:**
- Accumulators efficiently collect data from workers.
- Easy to implement in parallel applications without significant overhead.

**Limitations:**
- Accumulators are not guaranteed to be fault-tolerant for complex operations like non-commutative sums.
- They are only "addable" and cannot be modified after creation.

**Example:**
```python
from pyspark import SparkContext
sc = SparkContext("local", "Accumulator-Test")
accum = sc.accumulator(0)

def count_words(line):
    global accum
accum += len(line.split())

rdd = sc.parallelize(["For Testing Purpose : ", "Accumulators in PySpark : ", "Counting words"])
rdd.foreach(count_words)

print("Total word count using accumulator:", accum.value)
```

## 2. Broadcast Variables
Broadcast variables allow large data (like lookup tables) to be shared across all worker nodes, ensuring the data is not serialized multiple times, thus reducing network overhead.

It is Ideal for sharing read-only data across distributed nodes.

**Advantages:**
- Reduces data serialization overhead.
- Ensures that the data is shared across nodes efficiently.

**Limitations:**
- Broadcast variables are immutable, meaning they cannot be modified once distributed.
- If the data changes frequently, broadcasting becomes inefficient.

**Example:**
```python
large_data = {"Ram": 1, "Shyam": 2, "Mohan": 3}
broadcast_var = sc.broadcast(large_data)

rdd = sc.parallelize(["Ram", "Shyam", "Mohan", "Sohan"])
result = rdd.map(lambda x: broadcast_var.value.get(x, 0)).collect()

print("Result using broadcast variable:", result)
```

## 3. Deploying Applications
Deployment refers to the process of packaging, configuring, and executing applications in production environments.

Deploying Python applications in distributed environments (e.g., Spark clusters, cloud services).

**Advantages:**
- Ensures that applications are available in a production-ready state.
- Can be easily scaled using cloud or cluster-based deployment.

**Limitations:**
- Dependencies and version mismatches can cause deployment failures.
- Requires proper configuration for handling multiple environments (development, testing, production).

**Example:** Deploying a PySpark job on a cluster.
```bash
spark-submit --master yarn --deploy-mode cluster my_spark_test_job.py
```

## 4. Monitoring Applications
Monitoring involves tracking application performance, errors, and resource usage to ensure proper operation and optimization.

Useful for identifying bottlenecks, debugging, and ensuring resource efficiency in distributed applications.

To monitor a Spark Structured Streaming application, you need to keep an eye on various aspects like performance, latency, throughput, and resource utilization. Spark provides several ways to monitor and troubleshoot streaming applications, including the Spark UI, metrics, logs, and external tools.

We can monitor Spark Structured Streaming applications as below:

### 1. Using Spark UI
The Spark UI is one of the most important tools to monitor real-time performance and understand the behavior of Spark jobs.

**How to Access Spark UI:**
- If you're running Spark in local mode, the UI is usually available at http://localhost:4040.
- For cluster mode, you can access the Spark UI through the master or the resource manager (YARN, Kubernetes, etc.).

**Spark UI Components for Streaming:**
- **Jobs Tab:** Lists all the jobs and provides details like the number of stages, tasks, and the job's duration.
- **Stages Tab:** Shows how each stage is executed along with task distribution and shuffle details.
- **Streaming Tab:** Specific to streaming jobs, this tab shows:
  - **Input Rate:** Number of input records per second.
  - **Processing Rate:** Number of records processed per second.
  - **Batch Duration:** The time taken to process each micro-batch.
  - **Watermark:** Watermark levels used in event-time processing.
  - **Batch Statistics:** Status of each batch including duration, scheduling delay, and processing delay.

**Example:**
```
┌───────────────┬─────────────────┬──────────────────┬─────────────────┬──────────────────┐
│ Batch Time      Input Records/s  │ Processed Records/s │ Scheduling Delay │ Processing Time │
├───────────────┼─────────────────┼──────────────────┼─────────────────┼──────────────────┤
│ 2024-10-24    │ 1000             │ 950              │ 100 ms│ 300 ms│
└───────────────-┴─────────────────┴──────────────────┴─────────────────┴──────────────────┘
```

**Monitoring Key Metrics:**
- **Processing Time:** Time taken to process each batch.
- **Input Rate vs. Processing Rate:** If the input rate is consistently higher than the processing rate, your application might not keep up with the incoming data.
- **Scheduling Delay:** Indicates how long a batch waited to be processed. High scheduling delays can indicate system overload.
- **Watermarks:** Shows the event-time watermarks used to handle late data.

### 2. Using Spark Logs
Logs can give you detailed insights into the status of your application, errors, and progress of tasks.

**Steps to Enable Logs for Monitoring:**
- Configure log level to INFO or DEBUG in the log4j.properties file to get detailed logs:
```properties
log4j.rootCategory=INFO, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{HH:mm:ss} %-5p %c{1}:%L - %m%n
```

- Logs provide information about:
  - Batch processing start and end times.
  - **Errors:** Check for any task failures, data errors, or issues with external systems like Kafka or HDFS.
  - **Warnings:** Watch for warnings related to shuffle operations, memory limits, and GC pauses.

**Example:** When structured streaming query is started, it will log something like this:
```
10:20:00 INFO Query: Started Structured Streaming Query [id = ...] on [topic = ...]
10:20:05 INFO Query: Batch [2] processed with [1000] records in [300ms]
```

### 3. Using Metrics System
Spark's metrics system provides detailed insight into resource usage, including CPU, memory, and network metrics.

**Enabling Spark Metrics:** We can configure metrics to be exported to external monitoring systems such as Prometheus, Graphite, or Ganglia.

- To enable metrics, configure the metrics.properties file:
```properties
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=localhost
*.sink.graphite.port=2003
*.sink.graphite.period=10
*.sink.graphite.unit=seconds
*.sink.graphite.prefix=spark
```

**Key Metrics for Structured Streaming:**
- **Streaming metrics:**
  - `streaming.lastBatchProcessingTime`: Time taken to process the last batch.
  - `streaming.lastBatchInputRecords`: Number of records processed in the last batch.
  - `streaming.totalProcessedRecords`: Total records processed since the job started.

- **JVM metrics:**
  - `jvm.heapMemoryUsed`: Amount of heap memory used.
  - `jvm.gcTime`: Time spent in garbage collection.

### 4. Using External Monitoring Tools
- **Prometheus and Grafana:** You can export Spark metrics to Prometheus and visualize them with Grafana.
- **Ganglia:** Another tool to monitor system-level metrics like CPU, memory, and network usage for Spark clusters.
- **Cloud Monitoring:** If you are running Spark on cloud platforms like AWS or GCP, you can use their native monitoring tools:
  - **AWS CloudWatch** for monitoring your Spark cluster.
  - **GCP Stackdriver** for Google Cloud-based Spark jobs.

**Example Prometheus Metrics Setup:**
- In the metrics.properties file, add:
```properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics
```

- Once set up, we can scrape the metrics from the /metrics endpoint and visualize them in Prometheus or Grafana.

### 5. Monitoring Watermarking and Latency
If structured streaming query is using event-time processing with watermarks, it's important to monitor watermarks and event-time latencies.

**How to Monitor Watermark:**
- In the Spark UI, the Streaming Tab displays watermarks. If the watermark is delayed, it could mean that late data isn't arriving, or the system is behind in processing.

**Monitoring Event Latency:**
- Latency refers to the delay between when an event occurs and when it's processed. You can add custom metrics to monitor event-time latency by comparing the event timestamp with the processing time.

**Example:**
```python
query = df.writeStream \
.format("console") \
.trigger(processingTime="5 seconds") \
.outputMode("append") \
.start()

query.awaitTermination()

# We can monitor the query's status programmatically
print("Status:", query.status)
print("Recent progress:", query.lastProgress)
```

### 6. Monitoring Structured Streaming with Code
Spark provides an API to programmatically monitor and check the status of a streaming query.

**Query Progress:** The StreamingQuery object has methods like lastProgress and status to monitor real-time progress.

**Example:**
```python
query = df.writeStream.format("console").start()
# Check the status and progress every 5 seconds
import time
while query.isActive:
time.sleep(5)
print("Query Status:", query.status)
print("Last Progress:", query.lastProgress)
```

**Progress and Status Information:**
- **status:** Provides basic information like whether the query is active.
- **lastProgress:** Shows detailed statistics for the last batch, including the number of input rows, processing time, and batch duration.

**Summary:**
- Spark UI provides real-time monitoring for jobs, stages, and structured streaming batches.
- Logs help in identifying errors, progress, and performance bottlenecks.
- Metrics System gives detailed resource usage metrics, which can be exported to monitoring systems like Prometheus.
- External Tools like Prometheus, Grafana, or cloud-based monitoring platforms offer a more scalable solution for large deployments.
- Programmatically monitor the status and progress of streaming queries using Spark's API.

**Advantages:**
- Helps to detect performance issues early.
- Provides visibility into real-time operations.

**Limitations:**
- High overhead if too much logging is enabled.
- Requires careful selection of metrics to avoid excessive noise.

**Example:**
```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_data(data):
logger.info("Processing data: %s", data)
    return data

data = ["Test 1", "Test 2", "Test 3"]
for d in data:
process_data(d)
```

## 5. Performance Tuning
The process of optimizing application performance by reducing execution time, memory usage, and resource overhead.

**Usage:** Commonly used in big data processing to ensure efficient execution of distributed tasks.

**Advantages:**
- Increases throughput and reduces processing time.
- Helps to use resources more efficiently.

**Limitations:**
- Requires deep knowledge of the application and system architecture.
- Over-optimization can lead to complexity and maintenance issues.

**Example:** Tuning a PySpark job to optimize performance.
```python
spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "50").getOrCreate()
rdd = spark.sparkContext.parallelize(range(1, 1000), 10)
rdd = rdd.repartition(50)  # Reduce number of shuffles by tuning partitions
```

## 6. Reducing Batch Processing Times
Optimizing batch processing to minimize execution time while maintaining correctness.

**Usage:** In real-time systems or data pipelines that process large amounts of data in batches.

**Advantages:**
- Reduces latency and speeds up data processing.
- Efficient resource usage.

**Limitations:**
- Over-tuning can increase the risk of data loss or inconsistency.
- Might require complex configurations.

**Example:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("large_data.csv", inferSchema=True)
df = df.repartition(100)  # Reducing batch time by increasing parallelism
df.write.csv("output_path", mode="overwrite")
```

## 7. Setting the Right Batch Interval
The batch interval determines how often the data stream is processed in a real-time system.

**Usage:** Setting the correct interval ensures that the system balances latency and throughput.

**Advantages:**
- Provides control over how frequently data is processed.
- Helps to balance the load on the system.

**Limitations:**
- Too frequent intervals can cause system overload, while long intervals may increase latency.

**Example:**
```python
from pyspark.streaming import StreamingContext
sc = SparkContext(appName="StreamApp")
ssc = StreamingContext(sc, 5)  # Setting batch interval to 5 seconds
lines = ssc.socketTextStream("localhost", 9999)
lines.pprint()

ssc.start()
ssc.awaitTermination()
```

## 8. Memory Tuning
Adjusting memory management to avoid issues like out-of-memory errors, garbage collection overhead, and inefficient caching.

**Usage:** Memory tuning is essential in large-scale distributed applications to optimize performance.

**Advantages:**
- Prevents memory bottlenecks.
- Ensures efficient garbage collection and memory utilization.

**Limitations:**
- Incorrect tuning can cause memory leaks or performance degradation.

**Example:**
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.memory.fraction", "0.6") \
.getOrCreate()
```

## 9. Fault-tolerance Semantics
Fault-tolerance ensures that a system can recover from failures without data loss or inconsistency.

**Usage:** Techniques like checkpointing or lineage tracking are used to achieve fault tolerance.

**Advantages:**
- Ensures high availability and data integrity.
- Minimizes downtime and system failures.

**Limitations:**
- Adding fault tolerance mechanisms can introduce overhead.
- Not all systems have native fault-tolerance support.

**Example:**
```python
ssc.checkpoint("/tmp/spark_checkpoint")
rdd = ssc.textFileStream("/path/to/logs")
```