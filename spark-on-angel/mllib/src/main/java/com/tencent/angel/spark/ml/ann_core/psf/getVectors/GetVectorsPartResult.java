package com.tencent.angel.spark.ml.ann_core.psf.getVectors;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class GetVectorsPartResult extends PartitionGetResult {
    private Long2ObjectOpenHashMap<float[]> vec;

    public GetVectorsPartResult(Long2ObjectOpenHashMap<float[]> vec) {
        this.vec = vec;
    }

    public GetVectorsPartResult() {
        this(null);
    }

    public Long2ObjectOpenHashMap<float[]> getVec() {
        return vec;
    }

    public int size() {
        return this.vec.size();
    }

    @Override
    public void serialize(ByteBuf output) {
        int vecNum = vec.size();
        output.writeInt(vecNum);
        ObjectIterator<Long2ObjectOpenHashMap.Entry<float[]>> iter = vec.long2ObjectEntrySet().fastIterator();
        while(iter.hasNext()) {
            Long2ObjectOpenHashMap.Entry<float[]> entry = iter.next();
            output.writeLong(entry.getLongKey());
            float[] value = entry.getValue();
            int dim = value.length;
            output.writeInt(dim);
            for (int i = 0; i < dim; i += 1) {
                output.writeFloat(value[i]);
            }
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        int len = input.readInt();
        this.vec = new Long2ObjectOpenHashMap<>(len);
        for (int i = 0; i < len; i += 1) {
            long key = input.readLong();
            int dim = input.readInt();
            float[] value = new float[dim];
            for (int j = 0; j < dim; j += 1) {
                value[j] = input.readFloat();
            }
            this.vec.put(key, value);
        }
    }

    @Override
    public int bufferLen() {
        int len = 4;
        int vecNum = vec.size();
        len += 12 * vecNum;

        ObjectIterator<Long2ObjectOpenHashMap.Entry<float[]>> iter = vec.long2ObjectEntrySet().fastIterator();
        while(iter.hasNext()) {
            Long2ObjectOpenHashMap.Entry<float[]> entry = iter.next();
            float[] value = entry.getValue();
            int dim = value.length;
            len += dim * 4;
        }

        return len;
    }

}
