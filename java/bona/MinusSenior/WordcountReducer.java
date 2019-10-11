package bona.MinusSenior;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class WordcountReducer extends Reducer<KeyBean, SetBean, SetBean, NullWritable> {
    KeyBean flagBean = new KeyBean("a",0);
    @Override
    protected void reduce(KeyBean key, Iterable<SetBean> values, Context context)
            throws IOException, InterruptedException {
        Boolean flag = true;
        if(key.getFlag()==1){
            flagBean.setKey(key.getKey());
            flagBean.setFlag(key.getFlag());
        }
        if(key.getKey().equals(flagBean.getKey())&&flagBean.getFlag()==1)
            flag = false;

        if(flag){
            for (SetBean bean : values) {
                context.write(bean, NullWritable.get());
            }
        }
    }

}
