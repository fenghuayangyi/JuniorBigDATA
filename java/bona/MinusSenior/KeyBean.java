package bona.MinusSenior;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class KeyBean implements WritableComparable<KeyBean>  {
    private String key;
    private int flag;

    public KeyBean(){
        super();
    }

    public KeyBean(String key,  int flag) {
        this.key = key;
        this.flag = flag;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(key);
        out.writeUTF(String.valueOf(flag));
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.key = in.readUTF();
        this.flag = Integer.parseInt(in.readUTF());
    }
    @Override
    public String toString() {
        return key + " " + flag ;
    }

    public KeyBean copyBean(){
        KeyBean copyObj =  new KeyBean();
        copyObj.setKey(this.key);
        return copyObj;
    }
    // 二次排序
    @Override
    public int compareTo(KeyBean o) {
        return new CompareToBuilder()
                .append(key,o.getKey())
                .append(o.getFlag(),flag)
                .toComparison();
    }

    @Override
    public int hashCode() {
       return Objects.hash(key,flag);
    }
}
