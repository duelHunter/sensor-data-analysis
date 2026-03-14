package com.takehome.iot;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureAvgCombiner extends Reducer<IntWritable, SumCountWritable, IntWritable, SumCountWritable> {
    private final SumCountWritable outValue = new SumCountWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<SumCountWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        long count = 0L;

        for (SumCountWritable value : values) {
            sum += value.getSum();
            count += value.getCount();
        }

        outValue.set(sum, count);
        context.write(key, outValue);
    }
}
