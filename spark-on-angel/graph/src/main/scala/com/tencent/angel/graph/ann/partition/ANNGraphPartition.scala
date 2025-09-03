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

import com.tencent.angel.graph.ann.ANNPSModel
import com.tencent.angel.graph.ann.index.IndexGraph
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

abstract class ANNGraphPartition(sheetId: Int,
                                 partIndex: Int,
                                 val graphKeys: Array[Long],
                                 val graphIndexModel: IndexGraph)
  extends ANNPartition(sheetId, partIndex) with Serializable {
  
  def buildGraph(): ANNGraphPartition
  
  override def process(psModel: ANNPSModel, queryIds: Broadcast[Array[Long]],
                       topK: Int, batchSize: Int): Unit= {
    println(s"sheet $sheetId part $partIndex process ${queryIds.value.length} queries with ${graphIndexModel.distanceFunction.getName}")
    
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
        val neighbors = this.findNearest(value, topK)
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
  
  override def findNearest(vec: Array[Float], k: Int): Array[(Long, Float)] = {
    this.graphIndexModel.findNearest(vec, k).map(t => (t.getItem.getId, t.getDistance)).toArray
  }
  
  def processApproximation(psModel: ANNPSModel, topK: Int, batchSize: Int): Unit = {
    var batchCnt = 0
    var batchTime = System.currentTimeMillis()
    graphKeys.sliding(batchSize, batchSize).foreach { partQueries =>
      val t1 = System.currentTimeMillis()
      val searchRes = new ArrayBuffer[(Long, Array[(Long, Float)])]()
      for (qid <- partQueries) {
        val neighbors = graphIndexModel.findNeighbors(qid, topK)
        if (neighbors.nonEmpty) {
          val resK = neighbors.map(t => (t.getItem.getId, t.getDistance)).toArray
          searchRes += ((qid, resK))
        }
      }
      val t2 = System.currentTimeMillis()
      
      // push to ps
      psModel.updateSearchResult(searchRes.toArray, topK)
      val t3 = System.currentTimeMillis()
      
      println(s"part $partIndex, find part nearest cost: ${t2 - t1} ms, push and merge on ps cost: ${t3 - t2} ms.")
      
      batchCnt += 1
      if (batchCnt % 100 == 0) {
        println(s"part $partIndex, $batchCnt batches done, cost ${(System.currentTimeMillis() - batchTime) / 1000.0} s")
        batchTime = System.currentTimeMillis()
      }
    }
  }
}
