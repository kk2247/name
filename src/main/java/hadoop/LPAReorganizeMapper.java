package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author KGZ
 * @date 2019/4/16 9:35
 */
public class LPAReorganizeMapper extends Mapper<LongWritable,Text, Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name=value.toString().split("\t")[0];
        String content=value.toString().split("\t")[1];
        String label=name.split("#")[1];
        String name1=name.split("#")[0];
        String pageRank=content.split("#")[0];
        String link=content.split("#")[1];
        context.write(new Text(label),new Text(name1+"#"+pageRank+"#"+link));
    }
}
