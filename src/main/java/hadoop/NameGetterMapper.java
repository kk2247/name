package hadoop;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * @author KGZ
 * @date 2019/4/18 16:26
 */
public class NameGetterMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String nameList = context.getConfiguration().get("namelist");
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new FileReader(nameList));
        String nameline;
        while((nameline = br.readLine()) != null){
            DicLibrary.insert(DicLibrary.DEFAULT,nameline);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Result result = DicAnalysis.parse(line);
        List<Term> terms = result.getTerms();
        StringBuilder sb = new StringBuilder();
        if (terms.size()>0) {
            for (int i = 0; i < terms.size(); i++) {
                String word = terms.get(i).getName(); //拿到词
                String natureStr = terms.get(i).getNatureStr(); //拿到词性
                if (natureStr.equals("userDefine")) {
                    sb.append(word + " ");
                }
            }
        }
        String res =sb.length() > 0? sb.toString().substring(0,sb.length()-1):"";
        context.write(new Text(res),NullWritable.get());
    }
}
