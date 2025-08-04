package com.tencent.angel.spark.ml.ann_core;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ANNVector implements IElement {
    private float[] vector;

    public ANNVector(float[] vector) {
        this.vector = vector;
    }

    public ANNVector() {
        this(null);
    }

    public float[] getVector() {
        return this.vector;
    }

    public void setVector(float[] vector) {
        this.vector = vector;
    }

    @Override
    public Object deepClone() {
        float[] cloneVector = new float[vector.length];
        System.arraycopy(vector, 0, cloneVector, 0, vector.length);

        return new ANNVector(cloneVector);
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeFloats(output, vector);
    }

    @Override
    public void deserialize(ByteBuf input) {
        vector = ByteBufSerdeUtils.deserializeFloats(input);
    }

    @Override
    public int bufferLen() {
        return 4 + vector.length * 4;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        byte [] data = new byte[bufferLen()];
        int index = 0;
        output.writeInt(data.length);
        index = writeInt(data, vector.length, index);
        for (int i = 0; i < vector.length; i += 1) {
            index = writeFloat(data, vector[i], index);
        }

        output.write(data);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        byte[] data = new byte[input.readInt()];
        input.readFully(data);
        int index = 0;

        int inputVecLen = readInt(data, index);
        vector = new float[inputVecLen];
        index += 4;

        for (int i = 0; i < inputVecLen; i += 1) {
            vector[i] = readFloat(data, index);
            index += 4;
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

    private int writeInt(byte[] data, int v, int index) {
        data[index] = (byte)((v >>> 24) & 0xFF);
        data[index + 1] = (byte)((v >>> 16) & 0xFF);
        data[index + 2] = (byte)((v >>> 8) & 0xFF);
        data[index + 3] = (byte)((v >>> 0) & 0xFF);
        return index + 4;
    }

    private int writeFloat(byte[] data, float v, int index) {
        int iv = Float.floatToIntBits(v);
        return writeInt(data, iv, index);
    }

    private int readInt(byte[] data, int index) {
        int ch1 = data[index] & 255;
        int ch2 = data[index + 1] & 255;
        int ch3 = data[index + 2] & 255;
        int ch4 = data[index + 3] & 255;
        int c =  (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4);
        return c;
    }

    private float readFloat(byte[] data, int index) {
        return Float.intBitsToFloat(readInt(data, index));
    }

}
