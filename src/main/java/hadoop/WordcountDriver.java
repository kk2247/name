package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 */
public class WordcountDriver {
    static {
        try {
            System.load("C:/hadoop-2.7.6/bin/hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:/hadoop-2.7.6");
        if (args == null || args.length == 0) {
            return;
        }

        //该对象会默认读取环境中的 hadoop 配置。当然，也可以通过 set 重新进行配置
        Configuration conf = new Configuration();

        //job 是 yarn 中任务的抽象。
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\22478\\Desktop\\in"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\22478\\Desktop\\out"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
