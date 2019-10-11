package bona.MinusSenior;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text, KeyBean, SetBean>{
    SetBean bean = new SetBean();
    KeyBean k = new KeyBean();
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 1 获取输入文件类型
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        String line = value.toString();
        // 3 不同文件分别处理
        if (name.startsWith("Aset")) {// A集合处理
            String[] words = line.split(" ");
            // 封装bean对象
            bean.setKey(words[0]);
            bean.setValue(words[1]);
            bean.setFlag(0);
            k.setKey(words[0]);
            k.setFlag(0);
        }else {
            String[] words = line.split(" ");
            bean.setKey(words[0]);
            bean.setValue("0");
            bean.setFlag(1);
            k.setKey(words[0]);
            k.setFlag(1);
        }
        context.write(k, bean);
    }
}
