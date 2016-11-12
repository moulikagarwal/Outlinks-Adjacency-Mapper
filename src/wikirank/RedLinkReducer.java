package wikirank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedLinkReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        new StringBuilder();
        Set<String> set = new HashSet<String>();
        Iterator j = values.iterator();
        while ( j.hasNext()){
            //Add only unique links
            set.add(j.next().toString());
        }

//        if ( set.size() == 1){
//            //if the page is a sink
//            if (set.contains("#")){
//                output.collect(key, new Text("#"));
//            }
//
//        }else{

            if (set.contains("#")){
                Iterator i = set.iterator();
                while ( i.hasNext()){
                    String val = (String) i.next();
                    if ( !val.equals("#"))
                        context.write(new Text(val), key);
                    else {
                        context.write( key, new Text("#"));
                    }
                }
            }

        //}
    }
}