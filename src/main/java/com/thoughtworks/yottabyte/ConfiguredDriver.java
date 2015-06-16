package com.thoughtworks.yottabyte;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.mapsidestrategy.DenormalizingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.VEHICLES;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.VEHICLES_REPAIRS;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.VEHICLES_REPAIRS_OUTPUT;

public class ConfiguredDriver extends Configured implements Tool {
    public static final String COLUMN_SEPARATOR = "COLUMN_SEPARATOR";
    public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";
    private static ClassLoader loader;
    private Properties properties = new Properties();

    protected void loadPropertiesFile(String propertyFilePath) throws IOException {
        try (InputStream propertiesInputStream = new FileInputStream(propertyFilePath)) {
            properties.load(propertiesInputStream);
        } catch (NullPointerException npe) {
            System.out.println("No properties file found");
            System.exit(1);
        }
    }

    protected String get(String propertyName) {
        return Preconditions.checkNotNull(properties.getProperty(propertyName),
                "Expected %s to be present, but was not", propertyName);
    }

    protected Path getPath(String propertyName) {
        return new Path(get(propertyName));
    }

    @Override
    public int run(String[] args) throws Exception {
        loadPropertiesFile(args[0]);

        Configuration configuration = getConf();
        configuration.set(COLUMN_SEPARATOR, get(VEHICLES.columnSeparator()));
        configuration.set(VEHICLE_DATE_FORMAT, get(VEHICLES.dateFormat()));

        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

        FileInputFormat.setInputPaths(job, new Path(get(VEHICLES.path())));
        FileOutputFormat.setOutputPath(job, new Path(get(VEHICLES_REPAIRS_OUTPUT.path())));

        job.addCacheFile(URI.create(get(VEHICLES_REPAIRS.distributedCacheLocation())));

        job.setJarByClass(DenormalizingMapper.class);
        job.setMapperClass(DenormalizingMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        loader = ConfiguredDriver.class.getClassLoader();
        if (args.length < 1) {
            args = new String[]{loader.getResource("config.properties").getPath()};
        }
        int exitCode = ToolRunner.run(new Configuration(), new ConfiguredDriver(), args);
        System.exit(exitCode);
    }
}
