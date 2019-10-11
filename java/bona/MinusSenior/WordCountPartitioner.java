package bona.MinusSenior;

import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<KeyBean, SetBean>{
    @Override
    public int getPartition(KeyBean key, SetBean value, int numPartitions) {

        // 1 获取单词key的首字母
        int result  = key.getKey().charAt(0);

        // 2 根据首字母分区
        return (result & Integer.MAX_VALUE) % numPartitions;
    }
}

