
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashSet;
import java.util.Set;
import java.net.URI;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;


public class WordCount extends Configured implements Tool {
  // Thu vien ghi log slf4j
  private static final Logger logger = Logger.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // isSkip = true: co phan skip stop_words.
    boolean isSkip = false;

    // Bien dung de luu lai duong dan cua file stop_words.txt.
    String stopWordPath = "";
    // Bien de giu lai index cua skip trong args
    int indexSkip = 0;
    for (int i = 0; i < args.length; i += 1) {
      // Lay argument nam sau -skip la duong dan cua stop_words.txt.
      if ("-skip".equals(args[i])) {
        isSkip = true;
        indexSkip = i;
        i += 1;
        logger.info("Path of stop_words.txt: " + args[i]);
        stopWordPath = args[i];
      }
    }
    Job job = new Job(conf, "wordcount");

    // Gan bien he thong "wordcount.skip.patterns" = isSkip.
    job.getConfiguration().setBoolean("wordcount.skip.patterns", isSkip);

    // Neu co duong dan stop_word thi gan bien he thong "wordcount.cache.file" = stopWordPath.
    if (stopWordPath.length() > 0)
      job.getConfiguration().setStrings("wordcount.cache.file", stopWordPath);
    job.setJarByClass(this.getClass());

    // Neu indexSkip = 0 co nghia la nguoi dung khong nhap duong dan stop_words.txt,
    // nen gan indexSkip = args.length de xu ly duong dan input output
    if (indexSkip == 0)
      indexSkip = args.length;
    for (int i = 0; i < indexSkip - 1; i += 1) {
      FileInputFormat.addInputPath(job, new Path(args[i]));
    }
    FileOutputFormat.setOutputPath(job, new Path(args[indexSkip - 1]));

    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private boolean caseSensitive = false;

    // Bien chua cac tu trong stop_words.txt: o dang hashset (hash table)
    private Set<String> skippedPatterns = new HashSet<String>();
    private static final Pattern PATTERN = Pattern.compile("[a-zA-Z]+");

    protected void setup(Mapper.Context context)
        throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      // Gan bien caseSensitive bang bien he thong wordcount.case.sensitive, mac dinh la false
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);

      // Lay bien he thong 'wordcount.skip.patterns", mac dinh la false
      // Neu co skip patterns thi lay gia tri duong dan stop_words.txt tu "wordcount.cache.file"
      if (config.getBoolean("wordcount.skip.patterns", false)) {
        String[] path = config.getStrings("wordcount.cache.file", "");
        parseSkipFile(path[0]);
      }
    }

    // Ham co dau vao la duong dan stop_words.txt
    // Chuc nang: Parse cac tu trong file stop_words.txt bo vao skippedPatterns 
    // su dung FileSystem de lay file tu HDFS va doc bang BufferedReader
    private void parseSkipFile(String path) {
      try {
        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line = br.readLine();
        while (line != null){
            skippedPatterns.add(line);
            line = br.readLine();
        }
      } catch (IOException e) {
        System.err.println("Caught exception while parsing stop_words.txt file: '" + StringUtils.stringifyException(e));
      }
    }

    public void map(LongWritable offset, Text inputLine, Context output)
        throws IOException, InterruptedException {
      String line = inputLine.toString();
      if (!caseSensitive) {
        line = line.toLowerCase();
      }

      Matcher m = PATTERN.matcher(line);
      while (m.find()) {
        String word = m.group();
        // Neu word nam trong skippedPatterns thi bo qua
        if (skippedPatterns.contains(word))
          continue;

        Text outputWord = new Text(word);
        output.write(outputWord, one);
      }          
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text word, Iterable<IntWritable> counts, Context output)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      output.write(word, new IntWritable(sum));
    }
  }
}

