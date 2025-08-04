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

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.ann.params._
import com.tencent.angel.graph.ann.utils.DataUtils
import com.tencent.angel.graph.data.NeighborDistance
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.tencent.angel.spark.ml.graph.utils.params._

import scala.collection.mutable.ArrayBuffer

class HnswModel(override val uid: String) extends Serializable with HasTopK with HasVecSep
  with HasItemSep with HasItemIdCol with HasVectorCol with HasDistanceFunction with HasPartitionNum
  with HasPSPartitionNum with HasStorageLevel with HasQueryPath with HasIsInternalQuery with HasBatchSize
  with HasMaxItemCount with HasItemIdx with HasVecIdx with HasQueryPartitionNum
  with HasUseBalancePartition with HasBalancePartitionPercent with HasSheetsNum {

  def this() = this(Identifiable.randomUID("HnswModel"))

  var ss: SparkSession = _
  var KNN: Int = _
  var data: RDD[(Long, Array[Float])] = _
  var queryIds: RDD[Long] = _
  var psModel: HnswPSModel = _
  var querySheets: Array[RDD[Long]] = _
  var partitionSheets: RDD[(Int, HnswPartition)] = _

  def start(dataset: Dataset[_]): Unit = {
    this.ss = dataset.sparkSession
    this.KNN = if ($(isInternal)) $(topK) else $(topK) + 1
    data = DataUtils.loadFromDF(dataset, $(itemIdCol), $(vectorCol), $(vecSep), $(partitionNum))
    data.persist($(storageLevel))
    val numVectors = data.count()

    println(s"$numVectors items in total...")

    var (queryData, tQueryIds) =
      if ($(isInternal)) {
        val tQIdsRDD =
          if ($(queryPath) == null) {
            print(s"full vectors as queries. ")
            data.map(_._1)
          }
          else {
            val qs = DataUtils.loadIdQueries(ss, $(queryPath), $(queryPartitionNum))
            print(s"load query ids from ${$(queryPath)}. ")
            qs
          }
        (null, tQIdsRDD)
      }
      else {
        assert($(queryPath) != null, s"query path is required.")
        val queryDF = DataUtils.loadVectors(ss, $(queryPath), $(itemSep))
        val tQueryData = DataUtils.loadFromDF(queryDF, $(itemIdCol), $(vectorCol), $(vecSep), $(queryPartitionNum))
        val tQIdsRDD = tQueryData.keys
        println(s"load query ids and vectors from ${$(queryPath)}. ")
        (tQueryData, tQIdsRDD)
      }

    this.queryIds = tQueryIds
    if (!$(isInternal)) {
      val queryCnt = queryData.count()
      if (queryCnt < $(queryPartitionNum)) {
        println(s"find ${queryCnt} queries, which is less than query part num ${${queryPartitionNum}}, repartition queries to ${queryCnt} parts")
        setQueryPartitionNum(queryCnt.toInt)
        println(s"new query partition num is ${$(queryPartitionNum)}")
        queryData = queryData.repartition($(queryPartitionNum))
      }
      queryData.persist($(storageLevel))
      queryData.count()
    }

    this.queryIds.persist($(storageLevel))
    println(s"total ${this.queryIds.count()} queries loaded.")

    println(s"process with ${$(disFunc)} function")

    val minId = queryIds.min()
    val maxId = queryIds.max()

    println(s"queryMinId: $minId queryMaxId: $maxId")
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1, "HnswModel",
      SparkContext.getOrCreate().hadoopConfiguration)

    this.psModel = HnswPSModel.apply(modelContext, this.data, $(useBalancePartition), $(balancePartitionPercent))

    println("start init ps...")
    val timeInitPS = System.currentTimeMillis()

    if (!$(isInternal)) {
      this.psModel.initQueries(queryData)
      queryData.unpersist(blocking = false)
    }
    else {
      this.psModel.initQueries(this.data)
    }
    println(s"init ps cost: ${(System.currentTimeMillis() - timeInitPS) / 1000.0} s")

    println("save checkpoint...")
    val timeWriteCheckpoint = System.currentTimeMillis()
    psModel.checkpoint(0, Array("queryVector", "searchRes"))
    println(s"write checkpoint cost: ${(System.currentTimeMillis() - timeWriteCheckpoint) / 1000.0} s")

  }

  def setANNPartsWithSheets(parts: RDD[HnswPartition]): Unit = {
    this.makeSheets()
    this.partitionSheets = parts.map(p => (0, p))
    for (i <- 1 until $(sheetsNum)) {
      this.partitionSheets = this.partitionSheets.union(parts.map(p => (i, p.setSheetId(i))))
    }
    this.partitionSheets.persist($(storageLevel))
    println(s"origin parts num: ${parts.count()}, ${this.partitionSheets.count()} parts of ${$(sheetsNum)} sheets")
    this.data.unpersist(blocking = false)
  }

  def makeSheets(): Unit = {
    this.querySheets = this.queryIds.randomSplit(Array.fill($(sheetsNum))(1.0))
  }

  def processANNParts(): Unit = {
    assert(this.partitionSheets != null, s"need to set ann partition sheets")

    val brQIdsArray = this.querySheets.map(sheet => SparkContext.getOrCreate().broadcast(sheet.collect()))

    println(s"start to process nearest neighbors")
    val timeFindKNN = System.currentTimeMillis()
    this.partitionSheets.foreach(sheet => sheet._2.process(psModel, brQIdsArray(sheet._1), KNN, $(batchSize)))
    this.partitionSheets.unpersist(blocking = false)
    println(s"find knn with top ${$(topK)} cost: ${(System.currentTimeMillis() - timeFindKNN) / 1000.0} s")
  }

  def processANNParts(parts: RDD[HnswPartition]): Unit = {
    this.setANNPartsWithSheets(parts)
    this.processANNParts()
  }

  def saveKNNResults(): DataFrame = {
    println(s"start to make saving data")
    val timeMakeSaving = System.currentTimeMillis()

    val retRDD = HnswModel.save(this.psModel, this.queryIds, this.KNN, $(batchSize))
      .map(a => (a._1, a._2.map(t => s"${t.getNeighbor}:${t.getDistance}").mkString(" ")))
      .map(row => Row.fromSeq(Seq[Any](row._1, row._2)))
    println(s"make saving data cost: ${System.currentTimeMillis() - timeMakeSaving} ms")

    val schema = StructType(Seq(
      StructField("query", LongType, nullable = false),
      StructField("nbr:dis", StringType, nullable = false)))

    this.ss.createDataFrame(retRDD, schema)
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}

object HnswModel {
  def save(psModel: HnswPSModel, qIds: RDD[Long], topK: Int,
           batchSize: Int): RDD[(Long, Array[NeighborDistance])] = {
    qIds.mapPartitions{ partQ =>
      val partQIds = partQ.toArray
      var batchCnt = 0
      var batchTime = System.currentTimeMillis()
      partQIds.sliding(batchSize, batchSize).flatMap{ part =>
        val partRes = psModel.getNearestNeighbors(part, topK)
        val iter = partRes.entrySet().iterator()
        val savedPart = new ArrayBuffer[(Long, Array[NeighborDistance])]()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = entry.getKey
          val value = entry.getValue
          savedPart.append((key, value))
        }
        batchCnt += 1
        if (batchCnt % 500 == 0) {
          println(s"$batchCnt batches pulled for saving, cost ${(System.currentTimeMillis() - batchTime) / 1000.0} s")
          batchTime = System.currentTimeMillis()
        }
        savedPart.toIterator
      }
    }
  }
}
