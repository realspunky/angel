package com.tencent.angel.spark.ml.ann_core.psf.getSearchResult;

import com.tencent.angel.graph.data.NearestNeighborsIElement;
import com.tencent.angel.graph.data.NeighborDistance;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class GetSearchResult extends GetFunc {
    private int matrixId;

    public GetSearchResult(GetSearchResultParam param) {
        super(param);
        matrixId = param.getMatrixId();
    }

    public GetSearchResult() {
        super(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        GeneralPartGetParam param = (GeneralPartGetParam) partParam;
        KeyPart keyPart = param.getIndicesPart();
        long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis = new Long2ObjectOpenHashMap<>(nodeIds.length);

        for (int i = 0; i < nodeIds.length; i += 1) {
            NearestNeighborsIElement tv = (NearestNeighborsIElement)(row.get(nodeIds[i]));
            resKeysDis.put(nodeIds[i], tv.getFoundKeysDis());
        }

        return new GetSearchResultPartResult(resKeysDis);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        int size = 0;
        for (PartitionGetResult result: partResults) {
            size += ((GetSearchResultPartResult) result).size();
        }
        Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis = new Long2ObjectOpenHashMap<>(size);

        for (PartitionGetResult result: partResults) {
            GetSearchResultPartResult r = (GetSearchResultPartResult) result;
            resKeysDis.putAll(r.getResKeysDis());
        }
        return new GetSearchResultResult(resKeysDis);

    }

}
