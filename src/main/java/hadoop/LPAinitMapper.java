package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author KGZ
 * @date 2019/4/14 16:56
 */
public class LPAinitMapper extends Mapper<LongWritable, Text,Text,Text> {

    private static int label=1;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name=value.toString().split("\t")[0];
        String content=value.toString().split("\t")[1];
        context.write(new Text(name+"#"+label),new Text(content));
        label++;
    }
}
