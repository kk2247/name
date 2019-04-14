package hadoop;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author KGZ
 * @date 2019/4/14 15:15
 */
public class PageRankReducer extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double rate=0;
        String line="";
        for(Text newRate:values){
            String value=newRate.toString();
            if(value.charAt(0)=='#'){
                line=value;
            }else{
                rate+= Double.valueOf(newRate.toString());
            }
        }
        String newLine=String.valueOf(rate)+line;
        context.write(key,new Text(String.valueOf(rate)+line));
    }
}
