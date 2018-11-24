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
 *
 * Lyric data requires preprocessing to remove newlines within quoted lyrics columns, such that there 1 record per line.
 * This can be achieved with the following Unix command (Updated 11/24 for Windows newlines):
 *  gawk -v RS='"' 'NR % 2 == 0 { gsub(/(\r*\n)+/, " ") } { printf("%s%s", $0, RT) }' INPUT.csv > OUTPUT.csv
 *
 * A few carriage returns (\r) may slip past the above command, honestly not sure how. In these instances
 * I simply opened vim and ran :%s/\r//g but obviously this could also be done by sed.
 *
 * Command line args:
 *  * 1. Path to rank data directory
 *  * 2. Path to 380k lyrics
 *  * 3. Path to 55k lyrics
 *  * 4. Path to 500k lyrics
 *  * 4. Output path
 */

public class Join {

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
            System.out.println("Short record encountered in spotify mapper at byte offset " + key.get());
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
        System.out.println(e.getMessage() + " at spotify mapper byte offset " + key.get());
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

      clean = normalize(clean).replaceAll("[^\\p{ASCII}]", "");   //Replace all accented characters with standard ASCII a-zA-Z
      clean = clean.replaceAll("[^A-Za-z0-9 ]", "");              //Remove all characters that are non-alphanumeric or space
      clean = clean.replaceAll(" +", "-");                        //Replace all spaces (including consecutive) with a single dash

      return clean.toLowerCase();
    }
  }

  public static class lyricMapper55k
      extends Mapper<LongWritable, Text, Text, Text> {

    private Text artist = new Text();
    private Text track = new Text();
    private Text URL = new Text();
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

        //CSV Header: artist,song,link,text
        for (CSVRecord line : parsed) {

          //Check number of columns in the CSV record, discard those that are incomplete
          if (line.size() < 4) {
            System.out.println("Short record encountered in 55k mapper at byte offset " + key.get());
            return;
          }

          artist.set(sanitize(line.get(0)));
          track.set(sanitize(line.get(1)));
          URL.set(line.get(2));
          lyrics.set(
              normalize(line.get(3))                  //Replace all accented characters with standard ASCII a-zA-Z
              .replaceAll("[^A-Za-z0-9 ]", "")  //Remove all remaining non-alphanumeric characters
              .replaceAll(" +", " ")            //Replace repeated spaces with a single space
              .toLowerCase()
          );
        }
      } catch (Exception e){
        System.out.println(e.getMessage() + " at 55k mapper byte offset " + key.get());
        return;
      }

      outKey.set(track + "//" + artist);
      outVal.set(("l" + lyrics));

      if (outKey.equals("")) return;

      context.write(outKey, outVal);

    }

    private static String sanitize(String dirty){
      String clean = dirty;
      clean = normalize(clean);                             //Replace all accented characters with standard ASCII a-zA-Z
      clean = clean.replaceAll("[^A-Za-z0-9 ]", "");  //Remove all characters that are non-alphanumeric or space
      clean = clean.replaceAll(" +", "-");            //Replace all spaces (including consecutive) with a single dash

      return clean.toLowerCase();
    }
  }

  public static class lyricMapper380k
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
            System.out.println("Short record encountered in 380k mapper at byte offset " + key.get());
            return;
          }

          index.set(line.get(0));
          track.set(line.get(1));   //Data is already in proper form, no sanitation necessary
          year.set(line.get(2));
          artist.set(line.get(3));  //No sanitation necessary
          genre.set(line.get(4));
          lyrics.set(normalize(line.get(5)).replaceAll("[^A-Za-z0-9 ]", "").toLowerCase());
        }
      } catch (Exception e){
        System.out.println(e.getMessage() + " at 380k mapper byte offset " + key.get());
        return;
      }

      outKey.set(track + "//" + artist);
      outVal.set(("l" + lyrics));

      if (outKey.equals("")) return;

      context.write(outKey, outVal);
    }
  }

  public static class lyricMapper500k
      extends Mapper<LongWritable, Text, Text, Text> {

    private Text artist = new Text();
    private Text track = new Text();
    private Text lyrics = new Text();

    private Text outKey = new Text();
    private Text outVal = new Text();
    CSVFormat format = CSVFormat.DEFAULT;

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      //Skip file byte offset 0 (first line in CSV is the header)
      if (key.get() == 0 || value.toString().equals("")) return;

      try {
        CSVParser parsed = CSVParser.parse(value.toString(), format);

        //CSV Header: Band,Lyrics,Song
        for (CSVRecord line : parsed) {

          //Check number of columns in the CSV record, discard those that are incomplete
          if (line.size() < 3) {
            System.out.println("Short record encountered in 500k mapper at byte offset " + key.get());
            return;
          }

          artist.set(sanitize(line.get(0)));
          lyrics.set(
              normalize(line.get(1))                  //Replace all accented characters with standard ASCII a-zA-Z
              .replaceAll("[^A-Za-z0-9 ]", "")  //Remove all remaining non-alphanumeric characters
              .replaceAll(" +", " ")            //Replace repeated spaces with a single space
              .toLowerCase()
          );
          track.set(sanitize(line.get(2)));
    
          if (lyrics.toString().isEmpty()){
           throw new Exception("Zero length record after sanitize()");
          }
        }
      } catch (Exception e){
        System.out.println(e.getMessage() + " at 500k mapper byte offset " + key.get());
        return;
      }

      outKey.set(track + "//" + artist);
      outVal.set(("l" + lyrics));

      if (outKey.equals("")) return;

      context.write(outKey, outVal);

    }

    private static String sanitize(String clean){
      clean = normalize(clean);                             //Replace all accented characters with standard ASCII a-zA-Z
      clean = clean.replaceAll("[^A-Za-z0-9 ]", "");  //Remove all characters that are non-alphanumeric or space
      clean = clean.replaceAll(" +", "-");            //Replace all spaces (including consecutive) with a single dash

      return clean.toLowerCase();
    }
  }

  private static String normalize(String input){
    return Normalizer
        .normalize(input, Normalizer.Form.NFD)
        .replaceAll("[^\\p{ASCII}]", "");           //Replace all accented characters with standard ASCII a-zA-Z
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
        switch (record.charAt(0)){
          case 's':
            spotifyRecord = record.toString().substring(1); break;
          case 'l':
            lyricsRecord = record.toString().substring(1); break;
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

    MultipleInputs.addInputPath(job1, new Path(args[0]),  // /Spotify
        TextInputFormat.class, spotifyMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]),  // /380k
        TextInputFormat.class, lyricMapper380k.class);
    MultipleInputs.addInputPath(job1, new Path(args[2]),  // /55k
        TextInputFormat.class, lyricMapper55k.class);
    MultipleInputs.addInputPath(job1, new Path(args[3]),  // /500k
        TextInputFormat.class, lyricMapper500k.class);
    job1.setReducerClass(songReducer.class);

    FileOutputFormat.setOutputPath(job1, new Path(args[4]));  //Output directory
    job1.waitForCompletion(true);
  }

}
