package bona.WCPluss;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WCGroupingComparator extends WritableComparator {

    protected WCGroupingComparator() {
        super(TxtValue.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        TxtValue aBean = (TxtValue) a;
        TxtValue bBean = (TxtValue) b;

        return new CompareToBuilder()
                .append(aBean.getWords().charAt(0),bBean.getWords().charAt(0))
                .toComparison();

    }
}

