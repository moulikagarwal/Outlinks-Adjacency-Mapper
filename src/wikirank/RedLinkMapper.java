package wikirank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RedLinkMapper extends Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String page = value.toString();
        String txt = "";
        
        String articleTitle = page.substring(page.indexOf("<title>")+7, page.indexOf("</title>")).replace(" ", "_");
        int textStartIndex = page.indexOf("<text");
        int textEndIndex = page.indexOf("</text>");
        if ( textStartIndex != -1 && textEndIndex != -1)
            txt = page.substring(textStartIndex, textEndIndex );

        Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher m = p.matcher(txt);

        //This part handles the red links
        context.write(new Text(articleTitle), new Text("#"));

        while(m.find()) {

            String temp = m.group(1);

            if(temp != null && !temp.isEmpty())
            {
                if(temp.contains("|")){
                    String outlink = temp.substring(0, temp.indexOf("|")).replace(" ", "_");
                    if ( !outlink.equals(articleTitle))
                    	context.write(new Text(outlink), new Text(articleTitle) );
                }
                else{
                    String outlink = temp.replace(" ","_");
                    if ( !outlink.equals(articleTitle))
                    	context.write( new Text(outlink), new Text(articleTitle) );
                }
            }
        }
    }
}