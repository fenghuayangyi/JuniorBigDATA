package bona.WCPluss;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ValueBean implements WritableComparable<ValueBean> {
    private String words;

    public ValueBean() {
        super();
    }
    public ValueBean(String words) {
        super();
        this.words = words;
    }

    public String getWords() {
        return words;
    }

    public void setWords(String words) {
        this.words = words;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(words);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.words = in.readUTF();
    }
    @Override
    public String toString() {
        return words;
    }

    // 二次排序
    @Override
    public int compareTo(ValueBean o) {
        return new CompareToBuilder()
                .append(words,o.getWords())
                .toComparison();
    }

    @Override
    public int hashCode() {
        return Objects.hash(words);
    }

}
