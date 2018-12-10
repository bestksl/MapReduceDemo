package com.bestksl.page.topn.group;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements Writable, WritableComparable<OrderBean> {

    private String orderId;
    private String userId;
    private String pdtName;
    private float price;
    private int number;
    private float amountFee;


    public OrderBean() {
    }

    public OrderBean(String orderId, String userId, String pdtName, float price, int number) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.number = number;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPdtName() {
        return pdtName;
    }

    public void setPdtName(String pdtName) {
        this.pdtName = pdtName;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.pdtName);
        dataOutput.writeFloat(this.price);
        dataOutput.writeInt(this.number);
    }

    public float getAmountFee() {
        return amountFee;
    }

    public void setAmountFee(float amountFee) {
        this.amountFee = amountFee;
    }

    public void set(String orderId, String userId, String pdtName, float price, int number) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.number = number;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.pdtName = dataInput.readUTF();
        this.price = dataInput.readFloat();
        this.number = dataInput.readInt();
        this.amountFee = this.number * this.price;
    }


    @Override
    public int compareTo(OrderBean o) {

        return this.orderId.compareTo(o.getOrderId()) == 0 ? Float.compare(o.getAmountFee(), this.getAmountFee()) : this.orderId.compareTo(o.getOrderId());
    }

    @Override
    public String toString() {
        return this.orderId + " " + this.getNumber() + " " + this.pdtName + " ";
    }
}
