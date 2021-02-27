# Spark Cheat Sheet

## Playing With Airline Dataset

### Load Data into a Dataframr From a CSV File

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

df_af_acc_16.withColumn("PartitionId", spark_partition_id()).groupBy("partitionId").count().orderBy(asc("PartitionId")).show()
```

