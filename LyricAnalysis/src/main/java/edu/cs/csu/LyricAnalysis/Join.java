import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.mapred.OutputCollector;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class Join {

  //=== MAPPERS ===
  public static class spotifyMapper
      extends Mapper<Object, Text, Text, Text> {

    private Text artist = new Text();
    private Text track = new Text();
    private Text lyrics = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      String sentences[] = value.toString().split(","); //csv split


    }
  }

  public static class lyricMapper
      extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      String sentences[] = value.toString().split(","); //csv split

    }
  }

  //=== REDUCER ===
  public static class songReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text artist = new Text();
    private Text track = new Text();
    private Text lyrics = new Text();

    public void reduce(Text key, Iterable<IntWritable> count, Context context)
        throws IOException, InterruptedException {



    }
  }

  public static void main(String[] args) throws Exception {

    // Job 1: count word frequency
    Configuration job1Conf = new Configuration();
    Job job1 = Job.getInstance(job1Conf, "LyricJoin");
    job1.setJarByClass(Join.class);

    job1.setMapperClass(spotifyMapper.class);
    //job1.setCombinerClass(UnigramReducer.class);
    job1.setReducerClass(songReducer.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    MultipleInputs.addInputPath(job1, new Path(args[0]),
        TextInputFormat.class, spotifyMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]),
        TextInputFormat.class, lyricMapper.class);
    FileOutputFormat.setOutputPath(job1, new Path("Unigram"));
    job1.waitForCompletion(true);
  }

}
