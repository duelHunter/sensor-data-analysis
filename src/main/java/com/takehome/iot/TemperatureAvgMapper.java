package com.takehome.iot;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureAvgMapper extends Mapper<LongWritable, Text, IntWritable, SumCountWritable> {
    private final IntWritable outKey = new IntWritable();
    private final SumCountWritable outValue = new SumCountWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split("\\s+");
        if (parts.length < 8) {
            return;
        }

        try {
            int moteId = Integer.parseInt(parts[3]);
            double temperature = Double.parseDouble(parts[4]);

            outKey.set(moteId);
            outValue.set(temperature, 1L);
            context.write(outKey, outValue);
        } catch (NumberFormatException ex) {
            // Skip malformed lines.
        }
    }
}
