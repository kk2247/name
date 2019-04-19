package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author KGZ
 * @date 2019/4/18 16:27
 */
public class NameGetterDriver
    {
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
        conf.set("namelist","file//jinyong_all_person.txt");

        Job job = Job.getInstance(conf);

        job.setJarByClass(NameGetterDriver.class);

        job.setMapperClass(NameGetterMapper.class);
        job.setReducerClass(NameGetterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.addInputPath(job, new Path("result\\input1"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("result\\output0"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
