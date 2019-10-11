package bona.WCPlus;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word:words){
            context.write(new Text(word), new IntWritable(1));
            if(word.length()>1)
                context.write(new Text(word.substring(0,1)), new IntWritable(1));
        }
    }
}
