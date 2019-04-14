package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.AnsjUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class NameMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map阶段的业务逻辑就写在自定义的map()方法中 maptask会对每一行输入数据调用一次我们自定义的map()方法
     */
    @Override
//    hello,1 hello,1
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将maptask传给我们的文本内容先转换成String
        String line = value.toString();
        AnsjUtil ansjUtil=new AnsjUtil();
        ArrayList<String> names=ansjUtil.getPersonName(line);
        for(int i= 0;i<names.size();i++){
            for(int j=0;j<names.size();j++){
                if(i!=j){
                    String newString = names.get(i)+","+names.get(j);
                    context.write(new Text(newString), new IntWritable(1));
                }
            }
        }
    }

}