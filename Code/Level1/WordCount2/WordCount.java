import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
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

public class WordCount extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    
    for (int i = 0; i < args.length - 1; i += 1) {
      FileInputFormat.addInputPath(job, new Path(args[i]));
    }
    
    FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    // Bien caseSensitive = true: phan biet hoa thuong
    // Bien caseSensitive = false: khong phan biet hoa thuong
    private boolean caseSensitive = false;
    // Tao regular expression chi lay tu co ki tu [a..z], [A..Z]
    private static final Pattern PATTERN = Pattern.compile("[a-zA-Z]+");

    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {

      // caseSensitive duoc gan bang gia tri bien he thong -Dwordcount.case.sensitive o tren command
      // va mac dinh neu bien -Dwordcount.case.sensitive khong co gia tri thi gan bang false
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
    }

    public void map(LongWritable offset, Text inputLine, Context output)
        throws IOException, InterruptedException {
      String line = inputLine.toString();

      // Neu caseSensitive = false thi bien tat ca ki tu cua line thanh chu thuong
      if (!caseSensitive) {
        line = line.toLowerCase();
      }

      // Dung bien PATTERN de lay ra cac tu match voi regular expression
      Matcher m = PATTERN.matcher(line);
      // Su dung Macher.find() de truy xuat tung tu trong list cac matcher
      while (m.find()) {
        String word = m.group();
        
        Text outputWord = new Text(word);
        output.write(outputWord, one);
      }     
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text word, Iterable<IntWritable> counts, Context output)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts)
        sum += count.get();

      output.write(word, new IntWritable(sum));
    }
  }
}
