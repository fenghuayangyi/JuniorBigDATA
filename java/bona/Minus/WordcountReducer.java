package bona.Minus;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        boolean flag = true;
        for(IntWritable value:values){
            if(value.get()==0){
                flag = false;
            }
            count +=value.get();
        }
        if(flag){
            for(int i=0; i< count; i++){
                context.write(key, NullWritable.get());
            }
        }

    }

}
