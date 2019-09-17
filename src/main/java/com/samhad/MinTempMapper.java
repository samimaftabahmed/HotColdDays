package com.samhad;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MinTempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private Text text = new Text();
    private FloatWritable floatWritable = new FloatWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

        while (tokenizer.hasMoreElements()) {

            String[] split = tokenizer.nextToken().split("\\s+");

            if (!split[6].equals("-9999.0")) {

                text.set(App.getFormattedDate(split[1]));
                floatWritable.set(Float.parseFloat(split[6]));
                context.write(text, floatWritable);
            }
        }
    }
}
