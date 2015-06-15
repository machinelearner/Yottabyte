package com.thoughtworks.yottabyte.vehiclecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VehicleCountReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text vehicleType, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        IntWritable count = new IntWritable(0);
        for (IntWritable value : values) {
            count = new IntWritable(count.get() + value.get());
        }
        context.write(vehicleType, new Text(count.toString()));
    }
}









