package com.hspark.job.vo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/7 16:24.
 */
public class Pair implements Serializable, Writable {

    private final static long serialVersionUID = 0L;

    private String text;
    private Integer intCount;

    public Pair() {

    }

    public Pair(String text, Integer count) {
        this.text = text;
        this.intCount = count;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Integer getIntCount() {
        return intCount;
    }

    public void setIntCount(Integer intCount) {
        this.intCount = intCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(text);
        out.writeInt(intCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        text = in.readUTF();
        intCount = in.readInt();
    }


    @Override
    public String toString() {
        return "Pair{" +
                "text='" + text + '\'' +
                ", intCount=" + intCount +
                '}';
    }
}
