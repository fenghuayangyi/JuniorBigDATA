package bona.WCPluss;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text, TxtValue, ValueBean>{
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word:words){
            TxtValue bean = new TxtValue();
            ValueBean vBean = new ValueBean();
            bean.setWords(word);
            vBean.setWords(word);
            context.write(bean, vBean);
        }
    }
}
