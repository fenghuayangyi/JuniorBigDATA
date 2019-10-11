package bona.Minus;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 1 获取输入文件类型
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        String line = value.toString();
        String[] words = line.split(" ");
        // 3 不同文件分别处理
        if (name.startsWith("Aset")) {// 订单表处理
            for(String word:words){
                context.write(new Text(word), new IntWritable(1));
            }
        }else {
            for(String word:words){
                context.write(new Text(word), new IntWritable(0));
            }
        }

    }
}
