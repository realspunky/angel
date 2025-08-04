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
package com.tencent.angel.graph.ann.algo

import com.tencent.angel.graph.ann.{Item, ProgressListener}
import com.tencent.angel.graph.ann.index.HnswIndex
import com.tencent.angel.graph.ann.index.HnswIndex.HnswIndexBuilder
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

class HnswPartition(sheetId: Int,
                    partIndex: Int,
                    graphKeys: Array[Long],
                    hnswIndex: HnswIndex,
                    val vectors: Array[Array[Float]])
  extends Serializable {

  def buildGraph(): HnswPartition = {
    println(s"start building graph part")
    val timePartBuildGraph = System.currentTimeMillis()
    val items = graphKeys.zip(vectors).map { it =>
      new Item(it._1, it._2)
    }

    hnswIndex.addAll(items, new ProgressListener((done, max) =>
      println(s"$done of $max vectors have been added")))
    println(s"part $partIndex build graph index cost: ${(System.currentTimeMillis() - timePartBuildGraph) / 1000.0} s")
    new HnswPartition(sheetId, partIndex, graphKeys, hnswIndex, vectors)
  }

  def setSheetId(id: Int): HnswPartition = {
    new HnswPartition(id, partIndex, graphKeys, hnswIndex, vectors)
  }

  def process(psModel: HnswPSModel, queryIds: Broadcast[Array[Long]],
              topK: Int, batchSize: Int): Unit = {
    println(s"sheet $sheetId part $partIndex process ${queryIds.value.length} queries with ${hnswIndex.distanceFunction.getName}")

    var batchCnt = 0
    var batchTime = System.currentTimeMillis()
    queryIds.value.sliding(batchSize, batchSize).foreach { partQueries =>
      val t1 = System.currentTimeMillis()
      val queries = psModel.getQueries(partQueries)

      val t2 = System.currentTimeMillis()
      val iter = queries.entrySet().iterator()
      val searchRes = new ArrayBuffer[(Long, Array[(Long, Float)])]()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getKey
        val value = entry.getValue
        val neighbors = hnswIndex.findNearest(value, topK)
        if (neighbors.nonEmpty) {
          searchRes += ((key, neighbors))
        }
      }

      val t3 = System.currentTimeMillis()

      // push to ps
      psModel.updateSearchResult(searchRes.toArray, topK)
      val t4 = System.currentTimeMillis()

      println(s"part $partIndex, pull cost: ${t2 - t1} ms, find part nearest cost: ${t3 - t2} ms, push and merge on ps cost: ${t4 - t3} ms.")

      batchCnt += 1
      if (batchCnt % 100 == 0) {
        println(s"part $partIndex, $batchCnt batches done, cost ${(System.currentTimeMillis() - batchTime) / 1000.0} s")
        batchTime = System.currentTimeMillis()
      }
    }
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
