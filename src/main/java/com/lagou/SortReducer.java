package com.lagou;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SortReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {


    int sum = 1;

    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        for (LongWritable  v:values){
            context.write(new LongWritable(sum),key);
            sum+=1;
        }
    }

}
