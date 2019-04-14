package util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
/**
 * @author KGZ
 * @date 2019/4/14 9:49
 */
public class AnsjTest {



    public static void test() {
        //只关注这些词性的词
        Set<String> expectedNature = new HashSet<String>() {{
            add("nr");add("nr1");
        }};
        String str = "这少女姓郭，单名一个襄字，乃大侠郭靖和女侠黄蓉的次女，有个外号叫做“小东邪”。她一驴一剑，只身漫游，原想排遣心中愁闷，岂知酒入愁肠固然愁上加愁，而名山独游，一般的也是愁闷徒增。河南少室山山势颇陡，山道却是一长列宽大的石级，规模宏伟，工程着实不小，那是唐朝高宗为临幸少林寺而开凿，共长八里。郭襄骑着青驴委折而上，只见对面山上五道瀑布飞珠溅玉，奔泻而下，再俯视群山，已如蚁蛭。顺着山道转过一个弯，遥见黄墙碧瓦，好大一座寺院。" ;
        Result result = ToAnalysis.parse(str);
        System.out.println(result.getTerms());

        List<Term> terms = result.getTerms();
        System.out.println(terms.size());

        for(int i=0; i<terms.size(); i++) {
            //拿到词
            String word = terms.get(i).getName();
            //拿到词性
            String natureStr = terms.get(i).getNatureStr();
            if(expectedNature.contains(natureStr)) {
                System.out.println(word + ":" + natureStr);
            }
        }
    }

    public static void main(String[] args) {
        AnsjUtil ansjUtil=new AnsjUtil();
        ArrayList<String> name=ansjUtil.getPersonName("这少女姓郭，单名一个襄字，乃大侠郭靖和女侠黄蓉的次女，有个外号叫做“小东邪”。她一驴一剑，只身漫游，原想排遣心中愁闷，岂知酒入愁肠固然愁上加愁，而名山独游，一般的也是愁闷徒增。河南少室山山势颇陡，山道却是一长列宽大的石级，规模宏伟，工程着实不小，那是唐朝高宗为临幸少林寺而开凿，共长八里。郭襄骑着青驴委折而上，只见对面山上五道瀑布飞珠溅玉，奔泻而下，再俯视群山，已如蚁蛭。顺着山道转过一个弯，遥见黄墙碧瓦，好大一座寺院。");
        System.out.println();
    }
}
