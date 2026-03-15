# Large-Scale IoT Sensor Data Analysis Using Hadoop MapReduce

This project computes **average sensor readings per sensor node (moteid)** from the Intel Berkeley Research Lab dataset using native Hadoop MapReduce in Java.

## Dataset
Source: https://db.csail.mit.edu/labdata/labdata.html

Expected schema per line:
```
<date> <time> <epoch> <moteid> <temperature> <humidity> <light> <voltage>
```

## Build
```
mvn -q -DskipTests package
```

The JAR will be created at:
```
target/iot-temp-avg-1.0.0.jar
```

## Run (Local or Pseudo-Distributed Hadoop)
Example using HDFS paths:
```
hadoop fs -mkdir -p /datasets/iot
hadoop fs -put data.txt/data.txt /datasets/iot/

hadoop jar target/iot-temp-avg-1.0.0.jar \
  /datasets/iot/data.txt \
  /outputs/iot-sensor-avgs
```

Check output:
```
hadoop fs -cat /outputs/iot-sensor-avgs/part-r-00000 | head -n 10
```

## Output
Each output line is:
```
<moteid> <avg_temperature> <avg_humidity> <avg_light> <avg_voltage>
```
(Tab-separated)

## Notes
- Mapper, Reducer, and Driver are implemented with the Hadoop Java API.
- A custom Writable (`MetricsSumCountWritable`) is used to aggregate sums and count.
