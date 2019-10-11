package bona.MinusPlus;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SetBean implements Writable {
    private String key; // 产品名称
    private String value; // 产品名称
    private String flag;// 表的标记

    public SetBean(){
        super();
    }

    public SetBean(String key, String value, String flag) {
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

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
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
        out.writeUTF(flag);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.key = in.readUTF();
        this.value = in.readUTF();
        this.flag = in.readUTF();
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
}
