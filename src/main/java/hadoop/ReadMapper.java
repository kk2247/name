package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.AnsjUtil;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author KGZ
 * @date 2019/4/16 13:47
 */
public class ReadMapper extends Mapper<LongWritable, Text,Text,Text> {
    private static int id=1;

    private static AnsjUtil all=new AnsjUtil();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String content=value.toString();
        ArrayList<String> names=all.getPersonName(content);
        if(names.size()>0){
            StringBuilder line=new StringBuilder();
            for(int i=0;i<names.size()-1;i++){
                line.append(names.get(i)).append(" ");
            }
            line.append(names.get(names.size()-1));
            context.write(new Text(String.valueOf(id)),new Text(line.toString()));
        }
    }
}
