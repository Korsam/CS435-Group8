import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.math3.util.Pair;
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
        // Passes KEY: word VALUE: Song \t tf
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
            String val = value.toString();
            if(val.length()!=0){
                String[] input = val.split("\t");
                String title = input[0];
                String artist = input[1];
                String lyrics = input[2];
                String[] lyricSplit = lyrics.split("\\s+");
                HashMap<String,Double> counts = new HashMap<String,Double>();
                double max = Double.MIN_VALUE;
                for(String s:lyricSplit) {
                    if (counts.containsKey(s)) {
                        counts.put(s, counts.get(s));
                    }else {
                    	counts.put(s, 1.0);
                    }
                    if(counts.get(s)>max){
                        max = counts.get(s);
                    }
                }
                for(String s:counts.keySet()) {
                    double tf = 0.5 + 0.5 * (counts.get(s)/max);
                    context.write(new Text("s"), new Text("Song\t" + tf));
                }
            }
        }
    }

    public static class RegionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
            String[] split = value.toString().split("\t");
            if(split.length>0){
                //System.out.println(split[0]+"\t"+split[1]+"\t"+split[2]);
                context.write(new Text(split[1]), new Text(split[0]+"\t"+split[2]+"\t"+split[3]));
            }
        }
    }

    public static class CompareReducer extends Reducer<Text, Text, Text, Text>{
        public static HashMap<String,Double> out = new HashMap<String,Double>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
            HashMap<String, Pair<Double,Double>> words = new HashMap<String, Pair<Double,Double>>();
            double tf = 0.0;
            String k = key.toString().trim();
            String[] split = k.split("\t");
            for(Text t:values) {
                //song
                if (k.length() == 2) {
                    tf = Double.parseDouble(split[1]);
                } else {
                    words.put(split[0],new Pair<Double, Double>(Double.parseDouble(split[1]),Double.parseDouble(split[2])));
                }
            }

            for(String region:words.keySet()){
                double val = tf * words.get(region).getSecond() - words.get(region).getFirst();
                val = val * val;
                if(out.containsKey(region)) {
                    val += out.get(region);
                }
                out.put(region,val);
            }
        }

        public void cleanup(Context context) throws InterruptedException, IOException{
            int i = 0;
            while(i < 3) {
                String m = "";
                double min = Double.MAX_VALUE;
                for (String s : out.keySet()) {
                    if(out.get(s)<min){
                        min = out.get(s);
                        m = s;
                    }
                }
                if(min != Double.MAX_VALUE){
                    context.write(new Text(m),new Text(min+""));
                    out.remove(m);
                    i++;
                }else{
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Comparison");
        job.setJarByClass(Compare.class);
        job.setCombinerClass(CompareReducer.class);
        job.setReducerClass(CompareReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CompareMapper.class);
        MultipleInputs.addInputPath(job, new Path("Test/regions"), TextInputFormat.class, RegionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}