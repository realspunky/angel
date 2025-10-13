package com.tencent.angel.spark.ml.ann_core.psf.updateSearchResult;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class UpdateSearchResultPartParam extends GeneralPartUpdateParam {
    private int dim;

    public UpdateSearchResultPartParam(int matrixId, PartitionKey partKey,
                                       KeyValuePart keyValuePart, int dim) {
        super(matrixId, partKey, keyValuePart);
        this.dim = dim;
    }

    public UpdateSearchResultPartParam() {
        this(-1, null, null, 0);
    }

    public int getDim() {
        return dim;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        buf.writeInt(dim);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        dim = buf.readInt();
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + 4;
    }

}
