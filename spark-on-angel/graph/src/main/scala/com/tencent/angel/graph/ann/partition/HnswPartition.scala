/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.graph.ann.partition

import com.tencent.angel.graph.ann.index.HnswIndex.HnswIndexBuilder
import com.tencent.angel.graph.ann.{Item, ProgressListener}
import com.tencent.angel.graph.ann.index.{HnswIndex, IndexGraph}

class HnswPartition(sheetId: Int,
                    partIndex: Int,
                    graphKeys: Array[Long],
                    graphIndexModel: IndexGraph,
                    val vectors: Array[Array[Float]])
  extends ANNGraphPartition(sheetId, partIndex, graphKeys, graphIndexModel) with Serializable {
  
  def buildGraph(): HnswPartition = {
    println(s"start building graph part")
    val timePartBuildGraph = System.currentTimeMillis()
    val items = graphKeys.zip(vectors).map{ it =>
      new Item(it._1, it._2)
    }
    
    graphIndexModel.addAll(items, new ProgressListener((done, max) =>
      println(s"$done of $max vectors have been added")))
    println(s"part $partIndex build graph index cost: ${(System.currentTimeMillis() - timePartBuildGraph) / 1000.0} s")
    new HnswPartition(sheetId, partIndex, graphKeys, graphIndexModel, vectors)
  }
  
  override def setSheetId(id: Int): HnswPartition = {
    new HnswPartition(id, partIndex, graphKeys, graphIndexModel, vectors)
  }
  
}

object HnswPartition {
  def apply(index: Int, iter: Iterator[(Long, Array[Float])], indexBuilder: HnswIndexBuilder): HnswPartition = {
    val partData = iter.toArray
    val keys = partData.map(_._1)
    val vectors = partData.map(_._2)
    
    val hnswIndex = new HnswIndex()
    hnswIndex.hnswIndexBuilder(indexBuilder)
    
    new HnswPartition(0, index, keys, hnswIndex, vectors)
  }
  
}
