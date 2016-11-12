package wikirank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikiPageRanking extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WikiPageRanking(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1]+"/graph";
        String rankingPath = args[1]+"/temp";

        boolean isCompleted = redlinkJob(inputPath, rankingPath);
        if (!isCompleted) return 1;
        isCompleted = adjacencyJob(rankingPath, outputPath);

        if (!isCompleted) return 1;
        return 0;
    }


    public boolean redlinkJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job LinkJob = Job.getInstance(conf, "LinkJob");
        LinkJob.setJarByClass(WikiPageRanking.class);

        // Mapper
        FileInputFormat.addInputPath(LinkJob, new Path(inputPath));
        LinkJob.setInputFormatClass(XmlInputFormat.class);
        LinkJob.setMapperClass(RedLinkMapper.class);
        LinkJob.setMapOutputKeyClass(Text.class);

        // Reducer
        FileOutputFormat.setOutputPath(LinkJob, new Path(outputPath));

        LinkJob.setOutputKeyClass(Text.class);
        LinkJob.setOutputValueClass(Text.class);
        LinkJob.setReducerClass(RedLinkReducer.class);

        return LinkJob.waitForCompletion(true);
    }

    private boolean adjacencyJob(String inputPath, String outputPath) throws Exception {
    	Configuration conf = new Configuration();

        Job adjGraphJob = Job.getInstance(conf, "adjGraphJob");
        adjGraphJob.setJarByClass(WikiPageRanking.class);

        // Mapper
        FileInputFormat.addInputPath(adjGraphJob, new Path(inputPath));
        adjGraphJob.setMapperClass(AdjacencyMapper.class);
        adjGraphJob.setMapOutputKeyClass(Text.class);

        // Reducer
        FileOutputFormat.setOutputPath(adjGraphJob, new Path(outputPath));
        adjGraphJob.setOutputFormatClass(TextOutputFormat.class);

        adjGraphJob.setOutputKeyClass(Text.class);
        adjGraphJob.setOutputValueClass(Text.class);
        adjGraphJob.setReducerClass(AdjacencyReducer.class);

        return adjGraphJob.waitForCompletion(true);
    }


}
