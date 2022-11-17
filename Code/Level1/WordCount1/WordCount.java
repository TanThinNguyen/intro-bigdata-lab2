import java.io.IOException;
import java.util.regex.Pattern;
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

// Ke thua tu class Configured va interface Tool de dinh nghia cho Hadoop cach chay chuong trinh.
public class WordCount extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Dung ToolRunner tao 1 instance moi cua WordCount de chay MapReduce.
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    // Tao 1 instance Job moi.
    // Dung getConf() de lay configuration object cua class WordCount va dat ten cho Job la wordcount.
    Job job = Job.getInstance(getConf(), "wordcount");
    // Thiet lap Jar file cho class WordCount.
    job.setJarByClass(this.getClass());
    
    // Them duong dan toi input la cac argument tru argument cuoi cung trong command.
    for (int i = 0; i < args.length - 1; i += 1) {
      FileInputFormat.addInputPath(job, new Path(args[i]));
    }
    
    // Them duong dan toi output la argument cuoi cung trong command.
    FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
    
    // Thiet lap Map class, Reduce class tuong ung voi custom Map va Reduce class ben trong WordCount.
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    // Thiet lap output key o dang chuoi nen la class Text
    // va output value o dang so nguyen nen la class Intwritable.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Cho cho toi khi hoan thanh task, 0 la khong co loi xay ra, 1 la co loi.
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Tao bien one kieu IntWritable va co gia tri la 1
    private final static IntWritable one = new IntWritable(1); 
    
    // Tao WORD_BOUNDARY la regular expression de tach tu trong cau bang khoang trang, ki tu dac biet
    // Voi \s* la nhieu ki tu khoang trang, \b la bat buoc trung khop voi thanh phan truoc no.
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    // Dau vao cua Map la tung dong cua input, va dung Context de ghi output cho Map task.
    public void map(LongWritable offset, Text inputLine, Context output)
        throws IOException, InterruptedException {

      // Ep kieu cho inputLine tu Text sang String
      String line = inputLine.toString();

      // Tach line thanh nhieu tu với Pattern WORD_BOUNDARY.
      for (String word : WORD_BOUNDARY.split(line)) {
        // Neu word rong thi bo qua
        if (word.isEmpty())
          continue;

        // Tao 1 Text outputWord tu word 
        // roi ghi outputWord vao trong output voi key la outputWord va value la 1.
        Text outputWord = new Text(word);
        output.write(outputWord, one);
      }
    }
  }

  // Dau vao cua Reduce la (tu, mang chua cac so lan xuat hien),
  // va dung Context de ghi output cho Reduce task.
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text word, Iterable<IntWritable> counts, Context output)
        throws IOException, InterruptedException {
      // Tinh tong so lan xuat hien của word
      int sum = 0;
      for (IntWritable count : counts)
        sum += count.get();

      // Ghi vao output key la word, value la so lan xuat hien của word
      output.write(word, new IntWritable(sum));
    }
  }
}
