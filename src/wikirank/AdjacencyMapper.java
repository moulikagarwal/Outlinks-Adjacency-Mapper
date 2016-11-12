package wikirank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AdjacencyMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String svalues = value.toString();
        String[] array = svalues.split("\t");

        context.write(new Text(array[0]), new Text(array[1]));
    }
}