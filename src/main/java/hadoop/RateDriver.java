package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author KGZ
 * @date 2019/4/14 13:41
 */
public class RateDriver {
    static {
        try {
            System.load("C:/hadoop-2.7.6/bin/hadoop.dll");
//            System.load("D:\\software\\hadoop-2.7.6\\bin\\hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:/hadoop-2.7.6");
//        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.6");
        if (args == null || args.length == 0) {
            return;
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(RateDriver.class);
        job.setMapperClass(RateMapper.class);
        job.setReducerClass(RateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("result\\output1\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("result\\output2\\test1"));
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }


}
