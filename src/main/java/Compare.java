import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
                // Title index 1, Artist index 2
                String lyrics = input[2].replaceAll("[^A-Za-z0-9\\s+]", "").toLowerCase();
                String[] lyricSplit = lyrics.split("\\s+");
                HashMap<String,Double> counts = new HashMap<String,Double>();
                double max = Double.MIN_VALUE;
                for(String s:lyricSplit) {
                    if (counts.containsKey(s)) {
                        counts.put(s, counts.get(s)+1.0);
                    }else {
                    	counts.put(s, 1.0);
                    }
                    if(counts.get(s)>max){
                        max = counts.get(s);
                    }
                }
                for(String s:counts.keySet()) {
                    double tf = 0.5 + 0.5 * (counts.get(s)/max);
                    context.write(new Text(s), new Text("Song\t" + tf));
                }
            }
        }
    }

    public static class RegionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
            String[] split = value.toString().split("\\s+");
            if(split.length>0){
                context.write(new Text(split[0]), new Text(split[1]+"\t"+split[2]+"\t"+split[3]));
            }
        }
    }

    public static class CompareReducer extends Reducer<Text, Text, Text, Text>{
        public static HashMap<String,Double> out = new HashMap<String,Double>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
            HashMap<String, Pair<Double,Double>> words = new HashMap<String, Pair<Double,Double>>();
            double tf = 0.0;
            String k = key.toString().trim();
            // Read in all values
            for(Text t:values) {
            	String[] split = t.toString().split("\t");
                //song
                if (split.length == 2) {
                    tf = Double.parseDouble(split[1]);
                } else {
                	if(Character.isDigit(split[2].charAt(0))||split[2].charAt(0)=='-') words.put(split[0],new Pair<Double, Double>(Double.parseDouble(split[1]),Double.parseDouble(split[2])));
                	//else System.out.println("Err: "+t.toString());
                }
            }
            // Compute region values
            ArrayList<Double> vals = new ArrayList<Double>();
            int index = 0;
            for(String region:words.keySet()){
            	if(tf==0.0) continue;
            	double val = tf * words.get(region).getSecond() - words.get(region).getFirst();
                val = val * val;
                if(out.containsKey(region)) {
                    val += out.get(region);
                }
                out.put(region,val);
            }
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
        	for(String s:out.keySet()) {
            	context.write(new Text(s), new Text(Math.sqrt(out.get(s))+""));
            }
        }
    }
    
    public static class SumMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    	private HashMap<String,Double> vals = new HashMap<String,Double>();
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
            String[] split = value.toString().split("\t");
            if(split.length>0){
            	vals.put(split[0],Double.parseDouble(split[1]));
            }
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
        	for(int i = 0;i < 3;i++) {
        		String m = "";
        		double min = Double.MAX_VALUE;
        		for(String region:vals.keySet()) {
        			double v = vals.get(region);
        			if(v<min) {
        				min=v;
        				m=region;
        			}
        		}
        		if(min != Double.MAX_VALUE) context.write(new Text(m), new DoubleWritable(vals.remove(m)));
        	}
        }
    }
   

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Comparison");
        job.setJarByClass(Compare.class);
        //job.setCombinerClass(CompareReducer.class);
        job.setReducerClass(CompareReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CompareMapper.class);
        MultipleInputs.addInputPath(job, new Path("final.txt"), TextInputFormat.class, RegionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("intermediate"));
        job.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "ComparisonFinal");
        job2.setJarByClass(Compare.class);
        job2.setMapperClass(SumMapper.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path("intermediate"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
        
    }
}