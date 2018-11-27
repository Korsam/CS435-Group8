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
        private static Text song = new Text("Song");
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
                    context.write(song, new Text(s + "\t" + tf));
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
        private static HashMap<String, Double> words = new HashMap<String, Double>();
        private HashMap<Text, HashMap<String, Pair<Double, Double>>> regions = new HashMap<Text, HashMap<String, Pair<Double,Double>>>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
            String k = key.toString();
            for(Text t:values){
                context.write(key,t);
            }

            //Song
            if(k.equalsIgnoreCase("Song")){
                for(Text t:values){
                    String[] split = t.toString().split("\t");
                    words.put(split[0],Double.parseDouble(split[1]));
                }
            }else{
                HashMap<String, Pair<Double,Double>> wrds = new HashMap<String, Pair<Double,Double>>();
                for(Text t:values){
                    String[] split = t.toString().split("\t");
                    Double pop = Double.parseDouble(split[1]);
                    Double idf = Double.parseDouble(split[2]);
                    wrds.put(split[0], new Pair<Double,Double>(pop,idf));
                }
                regions.put(key,wrds);
            }
        }

        public void cleanup(Context context) throws InterruptedException, IOException{
            HashMap<String,Double> out = new HashMap<String,Double>();
            //Loop through each regions idf and pop values
            for(Text key:regions.keySet()){
                HashMap<String, Pair<Double,Double>> wrds = regions.get(key);
                double value = 0.0;
                //Loop through all the words in the song
                for(String s:words.keySet()){
                    if(wrds.get(key)==null) continue;
                    double val = words.get(s)*wrds.get(key).getSecond() - wrds.get(key).getFirst();
                    value =+ val*val;
                }
                out.put(key.toString(), Math.sqrt(value));
            }
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
        job.setReducerClass(CompareReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CompareMapper.class);
        File dir = new File("regions");
        for(String name: dir.list()){
            if(name.endsWith(".csv")){
                MultipleInputs.addInputPath(job, new Path(name), TextInputFormat.class, RegionMapper.class);
            }
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}