package com.takehome.iot;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureAvgReducer extends Reducer<IntWritable, MetricsSumCountWritable, IntWritable, Text> {
    private final Text outValue = new Text();

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

        if (count > 0) {
            double temperatureAvg = temperatureSum / count;
            double humidityAvg = humiditySum / count;
            double lightAvg = lightSum / count;
            double voltageAvg = voltageSum / count;

            outValue.set(temperatureAvg + "\t" + humidityAvg + "\t" + lightAvg + "\t" + voltageAvg);
            context.write(key, outValue);
        }
    }
}
