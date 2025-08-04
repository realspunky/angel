package com.tencent.angel.spark.ml.ann_core.psf.getVectors;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetVectorsResult extends GetResult {
    private Long2ObjectOpenHashMap<float[]> vector;
    public GetVectorsResult(Long2ObjectOpenHashMap<float[]> vector) {
        this.vector = vector;
    }

    public Long2ObjectOpenHashMap<float[]> getVector() {
        return vector;
    }
}
