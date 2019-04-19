package gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

/**
 * @author KGZ
 * @date 2019/4/16 13:42
 */
public class MyGexf  {
    public static void main(String[] args) {
        Gexf gexf = new GexfImpl();
        Calendar date = Calendar.getInstance();

        gexf.getMetadata()
                .setLastModified(date.getTime())
                .setCreator("Gephi.org")
                .setDescription("A Web network");
        gexf.setVisualization(true);

        Graph graph = gexf.getGraph();
        graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);

        AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
        graph.getAttributeLists().add(attrList);

        Attribute attUrl = attrList.createAttribute("class", AttributeType.INTEGER, "Class");
        Attribute attIndegree = attrList.createAttribute("pageranks", AttributeType.DOUBLE, "PageRank");

        HashMap<String ,String> map=new HashMap<String, String>();
        ArrayList<Node> nodes=new ArrayList<Node>();
        try {
            File file=new File("result\\output5\\part-r-00000");
            BufferedReader reader=new BufferedReader(new FileReader(file));
            String line="";
            int index=0;
            while((line=reader.readLine())!=null){
                String line1=line.split("\t")[0];
                String line2=line.split("\t")[1];
                String label=line1.split("#")[0];
                String name=line1.split("#")[1];
                String pageRank=line1.split("#")[2];
                Node node=graph.createNode(String.valueOf(index));
                node
                        .setLabel(name)
                        .getAttributeValues()
                        .addValue(attUrl,label)
                        .addValue(attIndegree,pageRank);
                nodes.add(node);
                map.put(name,String.valueOf(index));
                index++;
            }
            reader=new BufferedReader(new FileReader(file));
            while((line=reader.readLine())!=null){
                String name=line.split("\t")[0].split("#")[1];
                String line2=line.split("\t")[1];
                String newIndex=map.get(name);
                String[] links=line2.split(";");
                for(int i=0;i<links.length;i++){
                    String name2=links[i].split(":")[0];
                    String rate=links[i].split(":")[1];
                    String newIndex2=map.get(name2);
                    nodes.get(Integer.parseInt(newIndex)).connectTo(nodes.get(Integer.parseInt(newIndex2))).setWeight(Float.valueOf(rate));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        StaxGraphWriter graphWriter = new StaxGraphWriter();
        File f = new File("static_graph_sample.gexf");
        Writer out;
        try {
            out =  new FileWriter(f, false);
            graphWriter.writeToStream(gexf, out, "UTF-8");
            System.out.println(f.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
