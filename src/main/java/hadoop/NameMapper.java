package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.AnsjUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NameMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map阶段的业务逻辑就写在自定义的map()方法中 maptask会对每一行输入数据调用一次我们自定义的map()方法
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Set<String> set=new HashSet<String>();
        if(line.trim()!=""){
            String[] names=line.split(" ");
            for (int i=0;i<names.length;i++){
                if(set.contains(names[i])){

                }else{
                    set.add(names[i]);
                }
            }
            ArrayList<String> strings=new ArrayList<String>(set);
            if(strings.size()>1){
                for(int i= 0;i<strings.size();i++){
                    for(int j=0;j<strings.size();j++){
                        if(i!=j){
                            String newString = strings.get(i)+","+strings.get(j);
                            context.write(new Text(newString), new IntWritable(1));
                        }
                    }
                }
            }
        }
    }

}