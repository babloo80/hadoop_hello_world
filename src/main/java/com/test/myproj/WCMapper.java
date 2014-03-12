package com.test.myproj;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class WCMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key,
                  Text value,
                  OutputCollector<Text, IntWritable> output,
                  Reporter reporter) throws IOException {
    String[] words = value.toString().split(" ");
    for(String word: words) {
      output.collect(new Text(word.toLowerCase()), new IntWritable(1));
    }
  }
}
