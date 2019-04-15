package hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Author: KGZ
 * @Date: 2019/4/15 0015 21:51
 * @Version 1.8
 */
public class LPAIterationReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String,Double> map=new HashMap<String, Double>();
        String link="";
        String pageRank="";
        String label="";
        for(Text text:values){
            String name=key.toString();
            String content=text.toString();
            if(content.startsWith("@")){
                link=content.split("@")[1];
            }else if(content.startsWith("$")){
                pageRank=content.split("$")[1];
            }else if(content.startsWith("&")){
                label=content.split("&")[1];
            }else{
                String[] con=content.split("#");
                map.put(con[0],Double.valueOf(con[2]));
            }
        }
        List<Map.Entry<String, Double>> keyList = new LinkedList<Map.Entry<String, Double>>(map.entrySet());
        Collections.sort(keyList, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1,
                               Map.Entry<String, Double> o2) {
                if(o2.getValue().compareTo(o1.getValue())>0){
                    return 1;
                }else if(o2.getValue().compareTo(o1.getValue())<0){
                    return -1;
                }  else {
                    return 0;
                }
            }
        });
        String number=keyList.get(0).getKey();
        String key1=key.toString()+"#"+number;
        String content1=pageRank+"#"+link;
        context.write(new Text(key1),new Text(content1));
        System.out.println();
    }
}
