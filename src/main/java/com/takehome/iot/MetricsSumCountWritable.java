package com.takehome.iot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MetricsSumCountWritable implements Writable {
    private double temperatureSum;
    private double humiditySum;
    private double lightSum;
    private double voltageSum;
    private long count;

    public MetricsSumCountWritable() {
        this(0.0, 0.0, 0.0, 0.0, 0L);
    }

    public MetricsSumCountWritable(double temperatureSum, double humiditySum, double lightSum, double voltageSum, long count) {
        this.temperatureSum = temperatureSum;
        this.humiditySum = humiditySum;
        this.lightSum = lightSum;
        this.voltageSum = voltageSum;
        this.count = count;
    }

    public void set(double temperatureSum, double humiditySum, double lightSum, double voltageSum, long count) {
        this.temperatureSum = temperatureSum;
        this.humiditySum = humiditySum;
        this.lightSum = lightSum;
        this.voltageSum = voltageSum;
        this.count = count;
    }

    public double getTemperatureSum() {
        return temperatureSum;
    }

    public double getHumiditySum() {
        return humiditySum;
    }

    public double getLightSum() {
        return lightSum;
    }

    public double getVoltageSum() {
        return voltageSum;
    }

    public long getCount() {
        return count;
    }

    public void add(double temperature, double humidity, double light, double voltage) {
        this.temperatureSum += temperature;
        this.humiditySum += humidity;
        this.lightSum += light;
        this.voltageSum += voltage;
        this.count += 1L;
    }

    public void merge(MetricsSumCountWritable other) {
        this.temperatureSum += other.temperatureSum;
        this.humiditySum += other.humiditySum;
        this.lightSum += other.lightSum;
        this.voltageSum += other.voltageSum;
        this.count += other.count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(temperatureSum);
        out.writeDouble(humiditySum);
        out.writeDouble(lightSum);
        out.writeDouble(voltageSum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.temperatureSum = in.readDouble();
        this.humiditySum = in.readDouble();
        this.lightSum = in.readDouble();
        this.voltageSum = in.readDouble();
        this.count = in.readLong();
    }
}
