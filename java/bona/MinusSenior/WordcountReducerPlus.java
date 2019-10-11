package bona.MinusSenior;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducerPlus extends Reducer<KeyBean, SetBean, SetBean, NullWritable> {
    @Override
    protected void reduce(KeyBean key, Iterable<SetBean> values, Context context)
            throws IOException, InterruptedException {
            for (SetBean bean : values) {
                if(bean.getFlag()==1)
                    break;
                context.write(bean, NullWritable.get());
            }
    }

}
