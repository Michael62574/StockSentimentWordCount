import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class StockWordCount {

    public static class StockMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> stopWords = new HashSet<>();
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path stopWordPath = new Path(cacheFiles[0]);
                FileSystem fs = FileSystem.get(stopWordPath.toUri(), conf);
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(stopWordPath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // 按最后一个逗号分割（避免标题中含逗号）
            int lastComma = line.lastIndexOf(',');
            if (lastComma == -1) return;

            String text = line.substring(0, lastComma).trim();
            String label = line.substring(lastComma + 1).trim();

            // 只处理标签为 "1" 或 "-1"
            if (!("1".equals(label) || "-1".equals(label))) {
                return;
            }

            // 清洗：转小写，只保留字母和空格
            text = text.toLowerCase().replaceAll("[^a-z\\s]", " ");
            String[] words = text.split("\\s+");

            for (String word : words) {
                if (word.isEmpty() || stopWords.contains(word)) continue;
                outKey.set(label);
                outValue.set(word);
                context.write(outKey, outValue);
            }
        }
    }

    public static class StockReducer extends Reducer<Text, Text, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> mos;
        private IntWritable outValue = new IntWritable();

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> wordCount = new HashMap<>();
            for (Text word : values) {
                String w = word.toString();
                wordCount.put(w, wordCount.getOrDefault(w, 0) + 1);
            }

            // 转为列表并按频次降序排序
            List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCount.entrySet());
            list.sort(Map.Entry.<String, Integer>comparingByValue().reversed());

            // 确定输出文件名
            String outputName;
            if ("1".equals(key.toString())) {
                outputName = "positive";
            } else if ("-1".equals(key.toString())) {
                outputName = "negative";
            } else {
                return;
            }

            // 输出 Top 100
            int count = 0;
            for (Map.Entry<String, Integer> entry : list) {
                if (count >= 100) break;
                outValue.set(entry.getValue());
                mos.write(outputName, new Text(entry.getKey()), outValue);
                count++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: StockWordCount <input csv> <output dir> <stopword file>");
            System.exit(2);
        }

        // Add stopword file to DistributedCache
        DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(), conf);

        Job job = Job.getInstance(conf, "Stock Sentiment Word Count");
        job.setJarByClass(StockWordCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Enable MultipleOutputs
        MultipleOutputs.addNamedOutput(job, "positive", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "negative", TextOutputFormat.class, Text.class, IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}