package com.tencent.angel.spark.ml.ann_core.psf.updateVectors;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;
import com.tencent.angel.spark.ml.ann_core.ANNVector;

public class UpdateVectors extends UpdateFunc {

    public UpdateVectors(int matrixId, long[] nodes, ANNVector[] updates) {
        this(new UpdateVectorsParam(matrixId, nodes, updates));
    }

    public UpdateVectors(UpdateVectorsParam param) {
        super(param);
    }

    public UpdateVectors() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        GeneralPartUpdateParam param = (GeneralPartUpdateParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) param.getKeyValuePart();
        long[] nodes = split.getKeys();
        IElement[] vectors = split.getValues();
        row.startWrite();
        try {
            for (int i = 0; i < nodes.length; i += 1) {
                row.set(nodes[i], vectors[i]);
            }
        } finally {
            row.endWrite();
        }
    }
}
