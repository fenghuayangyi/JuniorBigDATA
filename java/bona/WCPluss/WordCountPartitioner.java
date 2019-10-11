package bona.WCPluss;

import bona.MinusSenior.KeyBean;
import bona.MinusSenior.SetBean;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<TxtValue, ValueBean>{
    @Override
    public int getPartition(TxtValue key, ValueBean value, int numPartitions) {

        // 1 获取单词key的首字母
        int result  = key.getWords().charAt(0);

        // 2 根据首字母分区
        return (result & Integer.MAX_VALUE) % numPartitions;
    }
}

