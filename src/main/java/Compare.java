import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Compare {

    public static class CompareMapper extends Mapper<Object, Text, Text, Text>{
        private static Text song = new Text("Song");
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
            String val = value.toString();
            if(val.length()!=0){
                String[] input = val.split("\\t");
                String title = input[0];
                String artist = input[1];
                String lyrics = input[2];
                String[] lyricSplit = lyrics.split("\\s+");
                HashMap<String,Integer> counts = new HashMap<String,Integer>();
                for(String s:lyricSplit) {
                    if (counts.containsKey(s)) {
                        counts.put(s, counts.get(s));
                    }else {
                    	counts.put(s, 1);
                    }
                }
                for(String s:counts.keySet()) {
                    context.write(song, new Text(s + "\t" + counts.get(s)));
                }
            }
        }
    }

    public static class RegionMapper extends Mapper<Object, Text, Text, Text> {
        private static Text region = new Text("Region");
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
            if(value.toString().length()!=0) context.write(region, value);
        }
    }

    public static class CompareReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
            String k = key.toString();
            for(Text t:values){
                context.write(key,t);
            }

            //Song
            if(k.equalsIgnoreCase("Song")){

            }else{

            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Comparison");
        job.setJarByClass(Join.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CompareMapper.class);
        if(args[1].equalsIgnoreCase("all")){
            File dir = new File("regions");
            for(String name: dir.list()){
                if(name.endsWith(".csv")){
                    MultipleInputs.addInputPath(job, new Path(name), TextInputFormat.class, RegionMapper.class);
                }
            }
        }else{
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RegionMapper.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}