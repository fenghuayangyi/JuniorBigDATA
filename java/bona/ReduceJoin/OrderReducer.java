package bona.ReduceJoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<OrderBean> orderBeans = new ArrayList<>();
        OrderBean pdBean = new OrderBean();

        for (OrderBean bean : values) {
            if ("0".equals(bean.getFlag())) {// 订单表
                OrderBean orderBean = new OrderBean();
				try {
					BeanUtils.copyProperties(orderBean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}

				orderBeans.add(orderBean);
			} else {// 产品表
				try {
					BeanUtils.copyProperties(pdBean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		for(OrderBean bean:orderBeans){
			bean.setPname(pdBean.getPname());
			context.write(bean, NullWritable.get());
		}
	}
}

