package bona.MinusPlus;

import bona.ReduceJoin.OrderBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class WordcountReducer extends Reducer<Text, SetBean, SetBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<SetBean> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<SetBean> setBeans = new ArrayList<>();
        SetBean bBean = new SetBean();
        Boolean flag = true;
        for (SetBean bean : values) {
            if ("0".equals(bean.getFlag())) {// A集合
                SetBean setBean = new SetBean();
                setBean = bean.copyBean();
                setBeans.add(setBean);
            } else {// B集合
                flag = false;
            }
        }
        if(flag){
            for(SetBean bean:setBeans){
                context.write(bean, NullWritable.get());
            }
        }
    }

}
