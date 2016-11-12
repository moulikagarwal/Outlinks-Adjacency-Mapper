package wikirank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class AdjacencyReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder temp = new StringBuilder();
        Iterator j = values.iterator();
        if ( key.toString().equals("#")){
            while(j.hasNext())
            {
                context.write(new Text(j.next().toString()), new Text(""));
            }

        }else{
            while(j.hasNext())
            {
                String val = j.next().toString();

                if ( !val.equals("#") )
                    temp.append(val + "\t");
            }

            if (temp.length() > 0)
                temp.deleteCharAt(temp.length() - 1);

            context.write(key, new Text(temp.toString()));
        }
    }
}