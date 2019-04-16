package util;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author KGZ
 * @date 2019/4/14 10:05
 */
public class AnsjUtil {

    private Set<String> allName;

    public AnsjUtil(){
        File file=new File("file//jinyong_all_person.txt");
        try {
            BufferedReader reader=new BufferedReader(new FileReader(file));
            allName=new HashSet<String>();
            String line="";
            while((line=reader.readLine())!=null){
                allName.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<String> getPersonName(String line) {
        ArrayList<String> names=new ArrayList<String>();
        Set<String> expectedNature = new HashSet<String>() {{
            add("nr");add("nr1");add("n");
        }};
        Result result = ToAnalysis.parse(line);
//        System.out.println(result.getTerms());
        List<Term> terms = result.getTerms();

        for(int i=0; i<terms.size(); i++) {
            String word = terms.get(i).getName();
            String natureStr = terms.get(i).getNatureStr();
            if(allName.contains(word)){
                int counter=0;
                for(String name:names){
                    if(name.equals(word)){
                        counter=1;
                    }
                }
                if(counter==0){
                    names.add(word);
                }
            }
        }
        return names;
    }
}
