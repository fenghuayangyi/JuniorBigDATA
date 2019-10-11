package bona.WCPluss;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<TxtValue, ValueBean, Text, IntWritable> {
    @Override
    protected void reduce(TxtValue key, Iterable<ValueBean> values, Context context)
            throws IOException, InterruptedException {
        Text fir = new Text(String.valueOf(key.getWords().charAt(0)));
        int count = 0;
        String word = "";
        int wordCount = 0;
        for(ValueBean value:values){
            if(word==""){
                word = value.getWords();
                wordCount = 1;
            }else if(value.getWords().equals(word)){
                wordCount += 1;
            }else if(!value.getWords().equals(word)){
                context.write(new Text(word), new IntWritable(wordCount));
                word = value.getWords();
                wordCount = 1;
            }
            count +=1;
        }
        context.write(new Text(word), new IntWritable(wordCount));
        context.write(fir, new IntWritable(count));
    }

}
