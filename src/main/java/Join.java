import java.io.IOException;
import java.text.Normalizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Due to Apache CSV library dependencies, the best way to create a jar from this is to run 'mvn clean package'
 * in project dir. The shade plugin will result in the generation of 2 jars in the /target directory with project folder.
 * The one with the 'uber-' prefix is gonna be a beefy sob but it's the one you want. It will have all
 * Maven dependencies bundled up with the jar so it will be more portable.
 */

public class Join {

  //=== MAPPERS ===
  public static class spotifyMapper
      extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * Spotify data requires preprocessing to remove newlines within quoted columns.
     * This can be achieved with the following Unix command:
     *  gawk -v RS='"' 'NR % 2 == 0 { gsub(/\n/, " ") } { printf("%s%s", $0, RT) }' INPUT.csv > OUTPUT.csv
     */

    private Text position = new Text();
    private Text artist = new Text();
    private Text track = new Text();
    private Text streams = new Text();
    private Text date = new Text();
    private Text region = new Text();
    private Text URL = new Text();

    private Text outKey = new Text();
    private Text outVal = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      //Skip file byte offset 0 (first line in CSV is the header)
      if (key.get() == 0 || value.toString().equals("")) return;

      try {
        CSVFormat format = CSVFormat.DEFAULT;
        CSVParser parsed = CSVParser.parse(value.toString(), format);

        //CSV Header: Position,Track Name,Artist,Streams,URL,Date,Region
        for (CSVRecord line : parsed) {
          if (line.size() < 7) {
            System.out.println("Short record encountered in spotifyMapper at byte offset " + key.get());
            return;
          }

          position.set(line.get(0));
          track.set(sanitize(line.get(1)));   //Sanitized field
          artist.set(sanitize(line.get(2)));  //Sanitized field
          streams.set(line.get(3));
          URL.set(line.get(4));
          date.set(line.get(5));
          region.set(line.get(6));
        }
      } catch (Exception e) {
        //I had a problem where the CSV parser would trip up on an invalid escape sequence
        //I couldn't find the sequence so instead we just skip this record
        System.out.println(e.getMessage() + " at spotify byte offset " + key.get());
        return;
      }

      outKey.set(track + "//" + artist);
      outVal.set("s" + position + "\t" + streams + "\t" + date + "\t" + region);

      if (outKey.equals("")) return;
      context.write(outKey, outVal);

      //Output (value tagged with "s" for Spotify record): {track/artist, position streams date region}
    }

    /**
     * Sanitize fields to help generate key values that will be compatible with the 380k lyric dataset
     * @param dirty - a field that may contain illegal characters for our key
     * @return - the sanitized field
     */
    private static String sanitize(String dirty){
      String clean = dirty;

      clean = Normalizer
          .normalize(clean, Normalizer.Form.NFD)
          .replaceAll("[^\\p{ASCII}]", "");           //Replace all accented characters with standard ASCII a-zA-Z
      clean = clean.replaceAll("[^A-Za-z0-9 ]", "");  //Remove all characters that are non-alphanumeric or space
      clean = clean.replaceAll(" +", "-");            //Replace all spaces (including consecutive) with a single dash

      return clean.toLowerCase();
    }
  }


  public static class lyricMapper
      extends Mapper<LongWritable, Text, Text, Text> {

    private Text index = new Text();
    private Text track = new Text();
    private Text year = new Text();
    private Text artist = new Text();
    private Text genre = new Text();
    private Text lyrics = new Text();

    private Text outKey = new Text();
    private Text outVal = new Text();


    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      //Skip file byte offset 0 (first line in CSV is the header)
      if (key.get() == 0 || value.toString().equals("")) return;

      try {
        CSVFormat format = CSVFormat.DEFAULT;
        CSVParser parsed = CSVParser.parse(value.toString(), format);

        //CSV Header: index,song,year,artist,genre,lyrics
        for (CSVRecord line : parsed) {

          //Check number of columns in the CSV record, discard those that are incomplete
          if (line.size() < 6) {
            System.out.println("Short record encountered in lyricMapper at byte offset " + key.get());
            return;
          }

          index.set(line.get(0));
          track.set(line.get(1));
          year.set(line.get(2));
          artist.set(line.get(3));
          genre.set(line.get(4));
          lyrics.set(line.get(5).replaceAll("[^A-Za-z0-9 ]", "").toLowerCase());
        }
      } catch (Exception e){
        System.out.println(e.getMessage() + " at Lyrics byte offset " + key.get());
        return;
      }

      outKey.set(track + "//" + artist);
      outVal.set(("l" + lyrics));

      if (outKey.equals("")) return;

      context.write(outKey, outVal);
    }
  }

  //=== REDUCER ===
  public static class songReducer
      extends Reducer<Text, Text, Text, NullWritable> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> records, Context context)
        throws IOException, InterruptedException {

      String spotifyRecord = "";
      String lyricsRecord = "";

      for (Text record : records) {
        if (record.charAt(0) == 's') {
          spotifyRecord = record.toString().substring(1);
        } else if (record.charAt(0) == 'l') {
          lyricsRecord = record.toString().substring(1);
        }
      }

      if (spotifyRecord.equals("") || lyricsRecord.equals("")) return;

      result.set(key + "\t" + spotifyRecord + "\t" + lyricsRecord);
      context.write(result, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {

    // Job 1: count word frequency
    Configuration job1Conf = new Configuration();
    Job job1 = Job.getInstance(job1Conf, "LyricJoin");
    job1.setJarByClass(Join.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job1, new Path(args[0]),
        TextInputFormat.class, spotifyMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]),
        TextInputFormat.class, lyricMapper.class);
    job1.setReducerClass(songReducer.class);

    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    job1.waitForCompletion(true);
  }

}
