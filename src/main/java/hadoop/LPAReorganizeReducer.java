package hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author KGZ
 * @date 2019/4/16 9:35
 */
public class LPAReorganizeReducer extends Reducer<Text, Text,Text,Text> {
    private static int label=1;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text text:values){
            String content=text.toString();
            String name=content.split("#")[0];
            String pageRank=content.split("#")[1];
            String link=content.split("#")[2];
            context.write(new Text(label+"#"+name+"#"+pageRank),new Text(link));
        }
        label++;
    }
}
