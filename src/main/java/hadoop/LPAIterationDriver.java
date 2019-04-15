package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author: KGZ
 * @Date: 2019/4/15 0015 21:51
 * @Version 1.8
 */
public class LPAIterationDriver {
    static {
        try {
//            System.load("C:/hadoop-2.7.6/bin/hadoop.dll");
            System.load("D:\\software\\hadoop-2.7.6\\bin\\hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
    public static void main(String[] args) throws Exception {
//        System.setProperty("hadoop.home.dir", "C:/hadoop-2.7.6");
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.6");
        if (args == null || args.length == 0) {
            return;
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LPAIterationDriver.class);
        job.setMapperClass(LPAIterationMapper.class);
        job.setReducerClass(LPAIterationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("result\\output3\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("result\\output4"));
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
