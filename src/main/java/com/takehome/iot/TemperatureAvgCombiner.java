package com.takehome.iot;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureAvgCombiner extends Reducer<IntWritable, MetricsSumCountWritable, IntWritable, MetricsSumCountWritable> {
    private final MetricsSumCountWritable outValue = new MetricsSumCountWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<MetricsSumCountWritable> values, Context context)
            throws IOException, InterruptedException {
        double temperatureSum = 0.0;
        double humiditySum = 0.0;
        double lightSum = 0.0;
        double voltageSum = 0.0;
        long count = 0L;

        for (MetricsSumCountWritable value : values) {
            temperatureSum += value.getTemperatureSum();
            humiditySum += value.getHumiditySum();
            lightSum += value.getLightSum();
            voltageSum += value.getVoltageSum();
            count += value.getCount();
        }

        outValue.set(temperatureSum, humiditySum, lightSum, voltageSum, count);
        context.write(key, outValue);
    }
}
