package com.zhan.cyberdb.storage;

/**
 * 元组 (Tuple) - 代表数据库表中的一行数据
 */
public class Tuple {
    // 这一行数据的真实字节内容
    private byte[] data;

    public Tuple(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }
    
    public int getLength() {
        return data.length;
    }
}