package com.samhad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class App extends Configured implements Tool {


    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new App(), args);
            System.exit(res);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Changes the format of date string from yyyyMMdd to MM-dd-yyyy.
     * @param inputDate a date String of the form yyyyMMdd
     * @return String
     */
    static String getFormattedDate(String inputDate) {

        DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("MM-dd-yyyy");

        return LocalDate.parse(inputDate, inputFormat).format(outputFormat);
    }


    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(this.getConf(), "Hot or Cold Day Job");
        job.setJarByClass(App.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MinTempMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MaxTempMapper.class);

        job.setReducerClass(TemperatureReducer.class);
        job.setNumReduceTasks(2);
//        job.setCombinerClass(TemperatureReducer.class);

        /* GETTING THE FOLLOWING ERROR IF COMBINER IS USED

        19/09/16 15:51:25 INFO mapreduce.Job: Task Id : attempt_1528714825862_158900_m_000000_2, Status : FAILED
Error: java.io.IOException: wrong value class: class org.apache.hadoop.io.Text is not class org.apache.hadoop.io.FloatWritable
        at org.apache.hadoop.mapred.IFile$Writer.append(IFile.java:194)
        at org.apache.hadoop.mapred.Task$CombineOutputCollector.collect(Task.java:1396)
        at org.apache.hadoop.mapred.Task$NewCombinerRunner$OutputConverter.write(Task.java:1713)
        at org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl.write(TaskInputOutputContextImpl.java:89)
        at org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer$Context.write(WrappedReducer.java:105)
        at com.samhad.TemperatureReducer.reduce(TemperatureReducer.java:31)
        at com.samhad.TemperatureReducer.reduce(TemperatureReducer.java:10)
        at org.apache.hadoop.mapreduce.Reducer.run(Reducer.java:171)
        at org.apache.hadoop.mapred.Task$NewCombinerRunner.combine(Task.java:1734)
        at org.apache.hadoop.mapred.MapTask$MapOutputBuffer.sortAndSpill(MapTask.java:1641)
        at org.apache.hadoop.mapred.MapTask$MapOutputBuffer.flush(MapTask.java:1492)
        at org.apache.hadoop.mapred.MapTask$NewOutputCollector.close(MapTask.java:729)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:799)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:341)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:164)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1920)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:158)
        */

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
