package bona.MinusSenior;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;

public class SetBean implements WritableComparable<SetBean>  {
    private String key; // 产品名称
    private String value; // 产品名称
    private int flag;// 表的标记

    public SetBean(){
        super();
    }

    public SetBean(String key, String value, int flag) {
        this.key = key;
        this.value = value;
        this.flag = flag;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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
        out.writeUTF(value);
        out.writeInt(flag);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.key = in.readUTF();
        this.value = in.readUTF();
        this.flag = in.readInt();
    }
    @Override
    public String toString() {
        return key + " " + value ;
    }

    public SetBean copyBean(){
        SetBean copyObj =  new SetBean();
        copyObj.setKey(this.key);
        copyObj.setValue(this.value);
        return copyObj;
    }
    // 二次排序
    @Override
    public int compareTo(SetBean o) {
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
