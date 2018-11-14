package com.my01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class My01Driver extends Configured implements Tool {


    public static class WordCountMap extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, one);
            }
        }
    }


    public static class WordCountReduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        //将jab 所用的参数都打印出来
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }

        Job job = new Job(conf);
        job.setJarByClass(My01Driver.class);
        job.setJobName("My01Driver");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int code=job.waitForCompletion(true)?0:-1;
        return(code);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new My01Driver(), args);
        System.exit(exitCode);

//        My01Driver  myo1=new My01Driver();
//        myo1.setConf(new Configuration());
//        GenericOptionsParser parser = new GenericOptionsParser(
//                myo1.getConf(), args);
//        String[] toolArgs = parser.getRemainingArgs();
//        for(String arg:toolArgs){
//            System.out.println(arg);
//        }

    }
}
