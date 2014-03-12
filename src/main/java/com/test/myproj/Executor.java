package com.test.myproj;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;

public class Executor {
  public static void main(String[] args) throws IOException {
    JobConf jobConf = new JobConf(Executor.class);
    jobConf.setJobName("WC Test");

    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(IntWritable.class);

    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);

    jobConf.setMapperClass(WCMapper.class);
    jobConf.setCombinerClass(WCReducer.class);
    jobConf.setReducerClass(WCReducer.class);

    FileInputFormat.addInputPath(jobConf, new Path(Executor.class.getResource("/sample_doc.txt").getFile()));
    FileOutputFormat.setOutputPath(jobConf, new Path("./output"));

    JobClient.runJob(jobConf);
  }
}
