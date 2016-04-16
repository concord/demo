Spark Streaming Results
=======================

## Time Window 

### First test
Maxed out our cluster to get to clusterwide ~100k downstream records/sec
from kafka. Out of 40 cores spark is using 36 of them (35 allocated). The test took
approx 45 minutes to complete. 
Cluster Cores: 30
ExecutorMem: 15G
Window Duration: 10s
Window Slide Interval: 1s
kafkaMaxRatePerPartition: 750

### Second Test
This test finished in 45 minutes. The only difference was a decrease in cluster
resources.
Cluster Cores: 21
ExecutorMem: 4G
Window Duration: 10s
Window Slide Interval: 1s
kafkaMaxRatePerPartition: 750

### Third test
For this final test I increased the number of kafka streams to 3 attempting to
emmulate the behavior of the concord tests. Even with maxRatePerPartition set as low
as 400, the streaming engine still could not hold up. Multiple failures were reported
via the spark UI page.
Cluster Cores: 21
ExecutorMem: 4G
Window Duration: 10s
Window Slide Interval: 1s
kafkaMaxRatePerPartition: 400

### Fourth Test
This test contained job failures so we stopped it before it finished. The Kafka
source was configured to read 1000 records per partition leading to too many queued
stages.

### Sixth Test
Consistent results 47 minutes to completion.

Spark Streaming Report
======================

## Cluster Environment

The Spark Streaming benchmark was performed on a 8 node Mesos cluster using GCE. We
decided to run Spark on Mesos due to our familiarity with Mesos. An additional 5
node Kafka cluster was created and prefilled with the liberty data set. All machines
are of the n1-standard-8 type each containing 8 CPUs and 30GB of RAM.

We decided to implement the time windowed counting benchmark. The source code
implements a topology that reads the liberty data set from kafka and counts unique
space delimited strings grouping by month and year. Results are grouped into 10s
windows overlapping every 1s.

## Benchmark Setup

The Spark cluster was configured to process micro-batches at a 1s interval. This
interval was chosen to achieve low latency performance. Also is is desirable to
have the batch interval be as close to the average processing time as possible, so
the processing pipeline can move as fast as possible without queueing up too many
stages of work. After a prelimiary run it was confirmed that a 1s batch interval
would be a good choice for this benchmark.

When deciding on a kafka integration approach we decided to use the direct approach
over the reciever based approach.
[here](http://spark.apache.org/docs/latest/streaming-kafka-integration.html)
With the direct approach this reciever will create as many RDD partitions as there
are kafka partitions to consume. Since our 'liberty' topic contains 144 partitions,
this will allow us to automatically achive a high degree of parallelism. Also this
approach has the benefit of being more efficent because of its ability to directly
communicate with the broker, without zookeeper. Offsets are tracked within Spark
checkpoints and are not reported back to zookeeper unless manually sent, which is
not necessary anyway for this benchmark.

## Benchmark and Results

The actual test is contained in the file TimeCount.scala. This class manipulates a
kafka direct stream representing all of the data in the liberty topic from the
first offset. Each record is parsed for validity and grouped by into a DStream of
(key, value) tuples. The key represents the log timestamp month and year; the value
being the log message itself. After this initial transformation the stream is grouped
by key and window so that the value for a particular key is now a list of log messages
that contain the same key, all within the given windowing intervals. A final map is
performed to remove any duplicates within the window and retrieve a total count of
unique logs.






