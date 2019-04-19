package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author: KGZ
 * @Date: 2019/4/15  21:51
 * @Version 1.8
 */
public class LPAIterationMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        万大平#389	0.002972842934319245#刘正风:0.4;刘菁:0.2;史登达:0.4;
        String line1=value.toString().split("\t")[0];
        String line2=value.toString().split("\t")[1];
        String PR = line2.split("#")[0];
        String name = line1.split("#")[0];
        String nameList = line2.split("#")[1];
        String label = line1.split("#")[1];
        StringTokenizer tokenizer = new StringTokenizer(nameList,";");
        while(tokenizer.hasMoreTokens()){
            String[] element = tokenizer.nextToken().split(":");
            context.write(new Text(element[0]),new Text(label+"#"+name));
        }
        context.write(new Text(name),new Text("#"+nameList));
        context.write(new Text(name),new Text("$"+label));
        context.write(new Text(name),new Text("@"+PR));
    }
}
