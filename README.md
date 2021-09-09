# Spark Cheat Sheet

* Spark can
  * read and write data in a variety of structured formats (e.g., JSON, Hive tables, Parquet, Avro, ORC, CSV)
  * query data using JDBC / ODBC connectors from RDBMSs, from Azure Blob storage, Azure Data Lake Storage Gen2, Azure Cosmos DB, Azure Synapse Analytics, Cassandra, Couchbase, ElasticSearch, MongoDB etc.
* `SparkSesion` is used to access Spark functionality. In a Spark shell or Databricks notebook, the `SparkSession` is automatically created and is accesible via a variable named `spark`
* Spark tables - Dataframes can be saved as persistent tables using the `saveAsTable`. Supports optimization techniques such as partitioning and bucketing. Like dataframes, tables and views can also be cached and uncached
  * Managed tables - Both table data and metadata are stored in Hive metastore. `DROP TABLE` will delete both the metadata and the table data. It is useful when Spark is used as a DB to visualize the data using a reporting tool as managed tables can be accessed using JDBC / ODBC connector
  * Unmanaged tables - Only the metadata are stored in the Hive metastore. The data is externally managed outside Spark. A `DROP TABLE` will delete only the table metadata keeping the data untouched. It is useful when Spark is used mainly to process a large volume of data in parallel and the data will stay in an external data store e.g. a data lake

```
df.write.format("parquet")
.repartition(13)
.partitionBy("src")
.bucketBy(4, "dst", "carrier")
.saveAsTable("flightsbkdc")
```
* `Dataframe` APIs `cache()` vs `persist()` - `cache()` always caches with the default storage level `MEMORY_AND_DISK`, whereas, `persist()` allows to specify the storage level 
* `Dataframe` APIs `repartition()` vs `coalesce()` - `repartition()` does a fresh repartitioning in memory and it can increase or decrease the number of partitions as indicated by the calling parameters. `coalesce()`, on the other hand`, avoids shuffling, and reduces the number of partitions to the number as indcated by the calling parameters
* `cache()` or (`persist()`) doesn't caches the dataframe immediately. Usually `cache()` is followed by an operation like `count()` to cache the data
* `partitionBy()` - 
  * `partitionBy()` allows writing out the content of a dataframe to disk in a partitioned directory structure, where the directories represent the individual values of the columns which the dataframe is partitioned on
  * One of the disadvantages with `partitionBy()` is, the number of partitions is completely based on the number of values of the partitioned fields. Unlike `repartition()` or `bucketBy()`, there is no way of specifying the maximum number of partitions. Thus it's not advisable to use `partitionBy` with a high-cardinality column
  * The `partitionBy()` function takes a list of one or more column names and a directory is created for each value of the column - the related data are written out in the files within the appropriate directories
  * The `partitionBy()` starts with a value of a partitioned column (say, country = "US", id `partitionBy("US)` is invoked) and iterates with all the values of the partitioned field. If, in the given example, all data with country = "US" are found in a single memory partition, only one file will be written in the directory country=US. If data with country = "US" are spread across 4 different memory partitions, 4 files will be written out in the directory
  * The size of the files can be controlled by the option `maxRecordsPerFile`. Everytime the number is exceeded while writing out new records, a new file will be created for subsequent records
* `bucketBy()` - 
  * `bucketBy()` allows specifying the maximum number of buckets, where each bucket can contain multiple values for the column, which the `bucketBy` is invoked on
  * `bucketBy()` hashes the column values to determine which bucket a value belongs to
  * For every memory partition, `bucketBy()` will write out all the buckets. Thus if there are 4 memory partitions and 5 buckets are to be created, a total of 20 files will be written out
  * Bucketing is supported only for Spark managed tables `saveAsTable()`
  * Bucketing helps avoiding reshuffle during joins. If a dataframe needs to be joined with other dataframe/s more than once on a given key, bucketing on the said key saves the shuffling step during the joins

## Playing With Dataset

### Load Data into a Dataframe From a CSV File

```
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

airlines_schema = StructType(
  [
    StructField("Date",DateType(),True),
    StructField("Time",StringType(),True),
    StructField("Location",StringType(),True),
    StructField("Operator",StringType(),True),
    StructField("Flight #",StringType(),True),
    StructField("Route",StringType(),True),
    StructField("Type",StringType(),True),
    StructField("Registration",StringType(),True),
    StructField("cn/In",StringType(),True),
    StructField("Aboard",IntegerType(),True),
    StructField("Fatalities",IntegerType(),True),
    StructField("Ground",IntegerType(),True),
    StructField("Summary",StringType(),True)]
)

df_af_acc_pt1 = spark.read.csv(
       "/opt/data/airlines-data/airlines_accident_part1.csv",
       header=True,
       dateFormat="MM/dd/yyyy",
       schema=airlines_schema)

df_af_acc_pt2 = spark.read.csv(
       "/opt/data/airlines-data/airlines_accident_part2.csv",
       header=True,
       dateFormat="MM/dd/yyyy",
       schema=airlines_schema)

```

### Change Field Names of a Data Frame

```
for c in df_af_acc_pt1.columns:
    df_af_acc_pt1 = df_af_acc_pt1.withColumnRenamed(c, c.replace(" ", ""))

for c in df_af_acc_pt2.columns:
    df_af_acc_pt2 = df_af_acc_pt2.withColumnRenamed(c, c.replace(" ", ""))
```

### Add a New Field to a Dataframe

```
from pyspark.sql.functions import *

df_af_acc_pt1 = df_af_acc_pt1.withColumn("Year", year("Date"))
```

### Write a Dataframe to a Parquet File

```
df_af_acc_pt1.write.parquet("airlines_accident.parquet")

df_af_acc_pt2.write.parquet("airlines_accident.parquet")

df_af_acc_pt2.write.mode("append").parquet("airlines_accident.parquet")
```

* The 1st command will create a folder named airlines_accident.parquet. The parquet file, a CRC file and other associated files will be created within it. Snappy compression will be used by default
* The second command will throw an error saying that the parquet file `airlines_accident.parquet` already exists
* The third command will "logically" append the data to the "file" `airlines_accident.parquet`. Note that the word "file" is written in quotes, because actually in the filesystem `airlines_accident.parquet` is a folder and within this folder we now have two parquet files = one is created by the 1st command and the second one is created by the 3rd command. So, the mode "append" has logically appended the data, but at a physical level a new parquet file has got created.

(Assuming the dataframe has only one partition. For each partition a separate parquet file will be created.)

### Repartition the Dataframe

```
from pyspark.sql.functions import *

df_af_acc_16 = df_af_acc_pt1.repartition(16, "Year")
df_af_acc_16.rdd.getNumPartitions()

df_af_acc_16 \
    .withColumn("PartitionId", spark_partition_id()) \
    .groupBy("partitionId") \
    .count() \
    .orderBy(asc("PartitionId")) \
    .show()
```

### Bucketing a Dataframe

```
from pyspark.sql.functions import *

df_covid = spark.read.csv(
       "/opt/data/covid-data/covid_19_data.csv",
       header=True,
       dateFormat="MM/dd/yyyy")

for c in df_covid.columns:
    df_covid = df_covid.withColumnRenamed(c, c.replace(" ", ""))

df_covid \
    .repartition(3, "Country/Region") \
    .write \
    .mode("append") \
    .format("parquet") \
    .bucketBy(5, "Country/Region") \
    .saveAsTable("CovidParquet")
```

### Partitioning a Dataframe

```
from pyspark.sql.functions import *

df_covid = spark.read.csv(
       "/opt/data/covid-data/covid_19_data.csv",
       header=True,
       dateFormat="MM/dd/yyyy")

for c in df_covid.columns:
    df_covid = df_covid.withColumnRenamed(c, c.replace(" ", ""))

df_covid \
    .repartition(3, "Country/Region") \
    .write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("Country/Region") \
    .saveAsTable("CovidParquet")
```

### Structured Streaming with Window & UDF

```
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType
from pyspark.sql.functions import window, col, sum, max
from pyspark.sql.functions import udf
import datetime

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", 1)

def roundDownDateTime(tm):
    tm -= datetime.timedelta(minutes = tm.minute % 10, seconds = tm.second)
    return tm

def roundUpDateTime(tm):
    tm += datetime.timedelta(minutes = 10)
    tm -= datetime.timedelta(minutes = tm.minute % 10, seconds = tm.second)
    tm -= datetime.timedelta(minutes = 1)
    return tm

roundDownDateTimeUdf = udf(roundDownDateTime, TimestampType())
roundUpDateTimeUdf = udf(roundUpDateTime, TimestampType())

rawcount_schema = StructType(
    [
        StructField("Timestamp",TimestampType(),True),
        StructField("Count",IntegerType(),True)
    ])

rawcount_read_streaming = spark \
    .readStream \
    .option("header", "true") \
    .schema(rawcount_schema) \
    .option("timestampFormat", "dd-MM-yyyy HH:mm:ss") \
    .csv("/opt/data/rawcount")

rawcount_read_streaming = rawcount_read_streaming \
    .withColumn("Date", rawcount_read_streaming["Timestamp"].cast(DateType())) \
    .withColumn("WindowStart", roundDownDateTimeUdf(rawcount_read_streaming["Timestamp"])) \
    .withColumn("WindowEnd", roundUpDateTimeUdf(rawcount_read_streaming["Timestamp"]))
    
rawcount_window_query = rawcount_read_streaming \
    .withWatermark("Timestamp", "2 minutes") \
    .groupBy(window(col("Timestamp"), "2 minutes")) \
    .agg(sum("Count").alias("TotalCount"), max("Date").alias("MaxDate"), max("Timestamp").alias("MaxTimestamp"), max("WindowStart").alias("WindowStart"), max("WindowEnd").alias("WindowEnd")) \
    .select("WindowStart", "WindowEnd", "MaxTimestamp", "MaxDate", "TotalCount" )

rawcount_write_streaming = rawcount_window_query \
    .writeStream \
    .option("parquet.block.size", 128) \
    .option("checkpointLocation", "/opt/spark-checkpoint") \
    .queryName("rawcountWindowQuery") \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("MaxDate") \
    .start("rawcount_aggregates")

rawcount_write_streaming.awaitTermination()

```

### Spark Batch with Window

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType

rawcount_schema = StructType(
    [
        StructField("Timestamp",TimestampType(),True),
        StructField("Count",IntegerType(),True)
    ])

rawcount_df = spark.read.csv(
       "/opt/data/rawcount/*.csv", \
       header=True, \
       schema=rawcount_schema, \
       timestampFormat="dd-MM-yyyy HH:mm:ss")

rawcount_df \
    .groupBy(window(col("Timestamp"), "10 minutes")) \
    .agg(sum("Count").alias("TotalCount"), max("Timestamp").alias("MaxTimestamp")) \
    .select("TotalCount", "MaxTimestamp") \
    .show()

