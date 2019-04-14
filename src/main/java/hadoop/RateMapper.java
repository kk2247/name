package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author KGZ
 * @date 2019/4/14 11:19
 */
public class RateMapper extends Mapper<LongWritable,Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] data=line.split("\t");
        String name=data[0];
        String time=data[1];
        String name1=name.split(",")[0];
        String name2=name.split(",")[1];
        String newString=name2+":"+time;
        context.write(new Text(name1),new Text(newString));
    }
}
