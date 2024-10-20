package com.example;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wordcount { // 基于hadoop示例改写

  public static class TokenizerMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // 停词表
    private Set<String> stopWords = new HashSet<>();


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String stopwordlist = conf.get("stopwordlist");
      Path path = new Path(stopwordlist);
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream in = fs.open(path);

      if (in == null) {
        throw new IOException("File not found: " + stopwordlist);
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = reader.readLine()) != null) {
        stopWords.add(line);
      }
      reader.close();
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String []line = value.toString().split(",");
      // 对line[1]列，去掉标点，转化为大写，忽略停词和空字符串
      String headline=line[1].replaceAll("[^a-zA-Z]", " ").toUpperCase();
      for(String word: headline.split(" ")){
        if(word.length()>0 && !stopWords.contains(word)){
          this.word.set(word);
          context.write(this.word, one);
        }
      }
      
    }
  }

  public static class IntSumReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<String, Integer> result= new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      // <key,sum>写入result
      result.put(key.toString(), sum);
    }

    @Override
    // 重写cleanup方法，将结果写入context
    protected void cleanup(Context context) throws IOException, InterruptedException {
      List<Map.Entry<String, Integer>> list = new ArrayList<>(result.entrySet());
      list.sort((o1, o2) -> (o2.getValue() - o1.getValue()));
      int rank=1;
      // 输出前100的高频词
      for (Map.Entry<String, Integer> entry : list) {
        String res=rank+ ": " +entry.getKey() + ", " + entry.getValue();
        if(rank>100){
          break;
        }
        context.write(new Text(res),null);
        rank++;
      }
      // for (Map.Entry<String, Integer> entry : list) {
      //   String res=rank+ ": " +entry.getKey() + ", " + entry.getValue();
      //   context.write(new Text(res),null);
      //   rank++;
      // }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // String[] otherArgs =
    //     new GenericOptionsParser(conf, args).getRemainingArgs();
    // if (otherArgs.length != 2) {
    //   System.err.println("Usage: wordcount <in> <out>");
    //   System.exit(2);
    // }
    // 设置stop-word-list的路径

    conf.set("stopwordlist", args[2]);
    Job job = new Job(conf, "word count");
    job.setJarByClass(wordcount.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}