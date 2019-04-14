package hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author KGZ
 * @date 2019/4/14 14:27
 */
public class PageRankMapper extends Mapper<LongWritable,Text,Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString().split("\t")[1];
        double rate= Double.parseDouble(line.split("#")[0]);
        String[] personRate=line.split("#")[1].split(";");
        for(int i= 0 ; i<personRate.length;i++){
            String name=personRate[i].split(":")[0];
            double newRate= Double.parseDouble(personRate[i].split(":")[1]);
            context.write(new Text(name),new Text(String.valueOf(newRate*rate)));
        }
        context.write(new Text(value.toString().split("\t")[0]),new Text("#"+line.split("#")[1]));
    }
}
