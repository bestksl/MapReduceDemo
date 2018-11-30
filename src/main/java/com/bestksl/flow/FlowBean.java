package com.bestksl.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private int upFlow;
    private String phone;
    private int downFlow;
    private int amountFlow;

    public FlowBean(String phone, int upFlow, int downFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.amountFlow = upFlow + downFlow;

    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getAmountFlow() {
        return amountFlow;
    }

    public void setAmountFlow(int amountFlow) {
        this.amountFlow = amountFlow;
    }

    /**
     * hadoop 序列化对象时调用的方法
     */

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(upFlow);
        dataOutput.write(downFlow);
        dataOutput.write(amountFlow);
        dataOutput.writeUTF(phone);

    }

    @Override
    public String toString() {
        return this.phone + " " + this.upFlow + " " + this.downFlow + " " + this.amountFlow;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.amountFlow = dataInput.readInt();
    }
}
