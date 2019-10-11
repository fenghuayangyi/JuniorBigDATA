package bona.MinusSenior;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.commons.lang.builder.CompareToBuilder;

public class WCGroupingComparator extends WritableComparator {

    protected WCGroupingComparator() {
        super(KeyBean.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        KeyBean aBean = (KeyBean) a;
        KeyBean bBean = (KeyBean) b;

        return new CompareToBuilder()
                .append(aBean.getKey().toCharArray()[0],bBean.getKey().toCharArray()[0])
                .toComparison();

    }
}

