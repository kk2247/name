package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.sql.Time;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;

/**
 * @author KGZ
 * @date 2019/4/14 15:20
 */
public class PageRankDriver {
    static {
        try {
            System.load("C:/hadoop-2.7.6/bin/hadoop.dll");
//            System.load("D:\\software\\hadoop-2.7.6\\bin\\hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:/hadoop-2.7.6");
//        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.6");
        if (args == null || args.length == 0) {
            return;
        }


        boolean res=false;
        for(int i = 1;i<=10;){
            String fileName="\\part-r-00000";
            String firstPath="result\\output2\\test"+String.valueOf(i);
            String lastPath="result\\output2\\test"+String.valueOf(i+1);
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(firstPath+fileName));
            FileOutputFormat.setOutputPath(job, new Path(lastPath));
            job.waitForCompletion(true);
//            write(firstPath+fileName,lastPath+fileName);


            i++;
        }
        System.exit(0);
    }

    public static void write(String firstFileName,String lastFileName){
        try {
            BufferedReader firstReader=new BufferedReader(new FileReader(firstFileName));
            BufferedReader laseReader=new BufferedReader(new FileReader(lastFileName));
            Map<String,String> map1=new HashMap<String, String>();
            Map<String,String> map2=new HashMap<String, String>();
            String line1="";
            String line2="";
            while((line1=firstReader.readLine())!=null){
                line2=laseReader.readLine();
                map1.put(line1.split("\t")[0],line1.split("\t")[1].split("#")[1]);
                map2.put(line2.split("\t")[0],line2.split("\t")[1]);
            }
            firstReader.close();
            laseReader.close();
            BufferedWriter writer=new BufferedWriter(new FileWriter(lastFileName));
            Iterator iter = map2.keySet().iterator();
            while (iter.hasNext()) {
                String key = String.valueOf(iter.next());
                String value=map2.get(key);
                String other=map1.get(key);
                String line=key+"\t"+value+"#"+other;
                writer.write(line+"\n");
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
