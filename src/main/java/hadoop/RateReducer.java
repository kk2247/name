package hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


/**
 * @author KGZ
 * @date 2019/4/14 11:25
 */
public class RateReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> names=new ArrayList<String>();
        ArrayList<Integer> times=new ArrayList<Integer>();
        for(Text text:values){
            String name=text.toString();
            names.add(name.split(":")[0]);
            times.add(Integer.valueOf(name.split(":")[1]));
        }
        int counter=0;
        for(int number:times){
            counter+=number;
        }
        StringBuilder builder=new StringBuilder();
        builder.append("0.1#");
        for(int i=0;i<names.size();i++){
            double rate=Double.valueOf(times.get(i))/Double.valueOf(counter);
            builder.append(names.get(i)+":").append(rate).append(";");
        }
        context.write(key,new Text(builder.toString()));
    }
}
