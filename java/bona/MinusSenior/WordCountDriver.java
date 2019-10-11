package bona.MinusSenior;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver{
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCountDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类/分区类以及分区对应的数量
        job.setMapperClass(WordcountMapper.class);
//        job.setReducerClass(WordcountReducer.class);
        job.setReducerClass(WordcountReducerPlus.class);
//        job.setPartitionerClass(WordCountPartitioner.class);
//        job.setNumReduceTasks(2);
        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(KeyBean.class);
        job.setMapOutputValueClass(SetBean.class);
        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(SetBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置reduce端的分组
        job.setGroupingComparatorClass(WCGroupingComparator.class);

        job.setPartitionerClass(WordCountPartitioner.class);
        //为了方便查看数据结果，暂时使用1个reduce
        job.setNumReduceTasks(1);
        // job.submit();  waitForCompletion包含submit
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}