package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: KGZ
 * @Date: 2019/4/15 0015 21:51
 * @Version 1.8
 */
public class LPAIterationMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line1=value.toString().split("\t")[0];
        String line2=value.toString().split("\t")[1];
        String name1=line1.split("#")[0];
        String label=line1.split("#")[1];
        String pageRank=line2.split("#")[0];
        String link=line2.split("#")[1];
        String[] names=link.split(";");
        context.write(new Text(name1),new Text("@"+link));
        context.write(new Text(name1),new Text("!"+pageRank));
        context.write(new Text(name1),new Text("&"+label));
        for(String name:names){
            context.write(new Text(name.split(":")[0]),new Text(label+"#"+name1+"#"+name.split(":")[1]));
        }
    }
}
