package com.lagou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//封装任务并提交运行
public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "SortDriver");
        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);

        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new Path("data")); //指定读取数据的原始路径
        FileOutputFormat.setOutputPath(job, new Path("output")); //指定结果数据输出路径
        final boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);

    }
}
