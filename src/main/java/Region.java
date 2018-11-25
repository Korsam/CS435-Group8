import java.io.IOException;
import java.io.StringWriter;
import java.text.Normalizer;

import java.util.HashMap;
import java.util.List;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Args: InputDir OutputDir
 *
 */

public class Region {

  //=== MAPPERS ===
  public static class spotifyMapper
      extends Mapper<LongWritable, Text, Text, Text> {

    private Text position = new Text();
    private Text artist = new Text();
    private Text track = new Text();
    private Text streams = new Text();
    private Text date = new Text();
    private Text region = new Text();
    private Text URL = new Text();

    private Text outKey = new Text();
    private Text outVal = new Text();

    //Position,Track Name,Artist,Streams,URL,Date,Region
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      //Skip file byte offset 0 (first line in CSV is the header)
      if (key.get() == 0 || value.toString().equals("")) return;

      //Parse record
      try {
        CSVFormat format = CSVFormat.DEFAULT;
        CSVParser parsed = CSVParser.parse(value.toString(), format);

        //CSV Header: Position,Track Name,Artist,Streams,URL,Date,Region
        for (CSVRecord line : parsed) {
          if (line.size() < 7) {
            System.out.println("Short record encountered in Spotify mapper at byte offset " + key.get());
            return;
          }

          position.set(line.get(0));
          track.set(line.get(1));
          artist.set(line.get(2));
          streams.set(line.get(3));
          URL.set(line.get(4));
          date.set(line.get(5));
          region.set(line.get(6));
        }
      } catch (Exception e) {
        //I had a problem where the CSV parser would trip up on an invalid escape sequence
        //I couldn't find the sequence so instead we just skip this record
        System.out.println(e.getMessage() + " at Spotify mapper byte offset " + key.get());
        return;
      }

      //Reassemble record
      try {
        StringWriter valString = new StringWriter();
        StringWriter keyString = new StringWriter();

        CSVPrinter keyCsv = new CSVPrinter(valString, CSVFormat.DEFAULT);
        CSVPrinter valCsv = new CSVPrinter(valString, CSVFormat.DEFAULT);

        keyCsv.printRecord(track, artist);
        valCsv.printRecord(position, streams, URL, date, region);

        outKey.set(keyString.toString());
        outVal.set(valString.toString());

        context.write(outKey, outVal);
      } catch (Exception e){
        System.out.println("Error creating CSV record from byte offset " + key.get());
      }

      //Output: {{track,artist}, {position, streams, URL, date, region}}
    }
  }

  //=== REDUCER ===
  public static class songReducer
      extends Reducer<Text, Text, Text, NullWritable> {

    private Text track = new Text();
    private Text artist = new Text();
    private Text position = new Text();
    private Text streams = new Text();
    private Text URL = new Text();
    private Text date = new Text();
    private Text region = new Text();

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> records, Context context)
        throws IOException, InterruptedException {

      CSVFormat format = CSVFormat.DEFAULT;
      CSVParser parsedKey = CSVParser.parse(key.toString(), format);

      List<CSVRecord> keyCols = parsedKey.getRecords();
      track.set(keyCols.get(0).toString());
      artist.set(keyCols.get(1).toString());

      //HashMap of regions with a key representing total streams within that region
      //HashMap<Text,List<Text[]>> regions = new HashMap<Text,List<Text[]>>();
      HashMap<Text,Long> regions = new HashMap<Text,Long>();

      //Loop through day trend records for this track
      for (Text day : records) {
        try {
          //CSV Header: position,streams,URL,date,region
          CSVParser parsedVal = CSVParser.parse(day.toString(), format);
          //Should only loop once since this is a single CSV line
          for (CSVRecord columns : parsedVal) {
            position.set(columns.get(0));
            streams.set(columns.get(1));
            URL.set(columns.get(2));
            date.set(columns.get(3));
            region.set(columns.get(4));
          }
        } catch (Exception e) {
          System.out.println(e.getMessage() + " at Spotify reducer for record: " + day.toString());
        }

        //Increment track's regional stream count by this day's stream count
        Long regionStreamCt = regions.containsKey(region) ? regions.get(region) : 0;
        regions.put(region, regionStreamCt + Long.parseLong(streams.toString()));
      }

      //Todo: iterate through regions and context.write() track,artist,TOTAL_STREAMS_IN_REGION,URL,region
    }
  }

  private static String normalize(String input){
    return Normalizer
        .normalize(input, Normalizer.Form.NFD)
        .replaceAll("[^\\p{ASCII}]", "");     //Replace all accented characters with standard ASCII a-zA-Z
  }

  private static String sanitize(String dirty) {
    return normalize(dirty)
        .replaceAll("[^\\p{ASCII}]","")       //Replace all accented characters with standard ASCII a-zA-Z
        .replaceAll("[^A-Za-z0-9 ]", "")      //Remove all characters that are non-alphanumeric or space
        .replaceAll(" +","-")                 //Replace all spaces (including consecutive) with a single dash
        .toLowerCase();
  }

  public static void main(String[] args) throws Exception {

    // Job 1: count word frequency
    Configuration job1Conf = new Configuration();
    Job job1 = Job.getInstance(job1Conf, "LyricJoin");
    job1.setJarByClass(Join.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    job1.setReducerClass(songReducer.class);

    FileOutputFormat.setOutputPath(job1, new Path(args[1]));  //Output directory
    job1.waitForCompletion(true);
  }
}


