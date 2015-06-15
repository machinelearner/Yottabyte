package com.thoughtworks.yottabyte.vehiclecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class VehicleCountDriverTest {
    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, Text> vehicleCountDriver;

    @Before
    public void setUp() {
        VehicleMapper mapper = new VehicleMapper();
        VehicleCountReducer reducer = new VehicleCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        vehicleCountDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void shouldProjectVehicleInfoToVehicleType() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("TRUCK,3021a8e,ROSS,2007-07-31"));
        mapDriver.withInput(new LongWritable(), new Text("BUS,cfff9fc,MONICA,2013-04-11"));
        mapDriver.withInput(new LongWritable(), new Text("CAR,3dea779,JOHN,2002-08-11"));
        mapDriver.withInput(new LongWritable(), new Text("CAR,blabl34,JOHN,2002-08-11"));
        mapDriver.withOutput(new Text("TRUCK"), new IntWritable(1));
        mapDriver.withOutput(new Text("BUS"), new IntWritable(1));
        mapDriver.withOutput(new Text("CAR"), new IntWritable(1));
        mapDriver.withOutput(new Text("CAR"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void shouldCollectCountOfVehicleType() throws IOException {
        reduceDriver.withInput(new Text("TRUCK"), Arrays.asList(new IntWritable(1)));
        reduceDriver.withInput(new Text("BUS"), Arrays.asList(new IntWritable(1)));
        reduceDriver.withInput(new Text("CAR"), Arrays.asList(new IntWritable(1), new IntWritable(1)));

        reduceDriver.withOutput(new Text("TRUCK"), new Text("1"));
        reduceDriver.withOutput(new Text("BUS"), new Text("1"));
        reduceDriver.withOutput(new Text("CAR"), new Text("2"));
        reduceDriver.runTest();
    }

    @Test
    public void shouldCollectCountPerVehicleType() {
        vehicleCountDriver.withInput(new LongWritable(), new Text("TRUCK,3021a8e,ROSS,2007-07-31"));
        vehicleCountDriver.withInput(new LongWritable(), new Text("BUS,cfff9fc,MONICA,2013-04-11"));
        vehicleCountDriver.withInput(new LongWritable(), new Text("CAR,3dea779,JOHN,2002-08-11"));
        vehicleCountDriver.withInput(new LongWritable(), new Text("CAR,blabl34,JOHN,2002-08-11"));

        vehicleCountDriver.withOutput(new Text("TRUCK"), new Text("1"));
        vehicleCountDriver.withOutput(new Text("BUS"), new Text("1"));
        vehicleCountDriver.withOutput(new Text("CAR"), new Text("2"));

    }

}