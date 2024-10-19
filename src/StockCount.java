import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class StockCount {
    public static class StockCountMapper extends
        Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable code = new IntWritable(1);
        private Text stock = new Text();
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            // Split the string separated by commas
            String[] stockData = value.toString().split(",");
            // 去掉多余空格
            if(stockData.length > 3) {
                stock.set(stockData[stockData.length - 1].trim());
                context.write(stock, code);
            }

        }
    }
    public static class StockCountReducer extends
        Reducer<Text, IntWritable, Text, Text> { // 仿照WordCount模板
            // 保存股票代码和计数
            private Map<String, int> CountMap= new HashMap<>();
            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                CountMap.put(key.toString(), sum);
            }

        }
        // 重写cleanup方法，将结果写入context
        public void cleanup(Context context) throws IOException, InterruptedException {
            // 将value从大到小排序
            List<Map.Entry<String, int>> list = new ArrayList<>(CountMap.entrySet());
            list.sort((o1, o2) -> (o2.getValue() - o1.getValue()));
            

            int rank=1;
            for (Map.Entry<String, int> entry : list) {
                String res=rank+ ": " +entry.getKey() + ", " + entry.getValue();
                context.write(new Text(res),null);
                rank++;
            }
        }
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
              System.err.println("Usage: wordcount <in> <out>");
              System.exit(2);
            }
            Job job = new Job(conf, "stock count");
            job.setJarByClass(StockCount.class);
            job.setMapperClass(StockCountMapper.class);
            job.setReducerClass(StockCountReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
