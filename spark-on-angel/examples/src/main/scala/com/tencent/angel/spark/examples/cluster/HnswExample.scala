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
package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.graph.ann.algo.Hnsw
import com.tencent.angel.graph.ann.utils.DataUtils
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object HnswExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val vectorPath = params.getOrElse("vectorPath", null)
    val outputPath = params.getOrElse("outputPath", null)
    val queryPath = params.getOrElse("queryPath", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt

    val batchSize = params.getOrElse("batchSize", "2000").toInt
    val topK = params.getOrElse("topK", "10").toInt
    val isInternal = params.getOrElse("isInternal", "false").toBoolean
    val distanceFuncion = params.getOrElse("distanceFunction", "cosine-distance")
    val sheetsNum = params.getOrElse("sheetsNum", "1").toInt
    val queryPartitionNum = params.getOrElse("queryPartitionNum", "10").toInt

    val itemIdx = params.getOrElse("itemIdx", "0").toInt
    val vecIdx = params.getOrElse("vecIdx", "1").toInt

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val itemSep = params.getOrElse("itemSep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
    }
    val vecSep = params.getOrElse("vecSep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
    }
    val saveItemSep = params.getOrElse("saveItemSep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val ef = params.getOrElse("ef", "40").toInt
    val efConstruction = params.getOrElse("efConstruction", "40").toInt
    val M = params.getOrElse("M", "16").toInt
    val maxM = params.getOrElse("maxM", s"$M").toInt
    val maxM0 = params.getOrElse("maxM0", s"${2 * M}").toInt
    val mL = params.getOrElse("mL", s"${1 / Math.log(M)}").toDouble

    val hnsw = new Hnsw()
      .setTopK(topK)
      .setStorageLevel(storageLevel)
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psPartitionNum)
      .setIsInternal(isInternal)
      .setQueryPath(queryPath)
      .setDisFunc(distanceFuncion)
      .setItemSep(itemSep)
      .setItemIdx(itemIdx)
      .setVecSep(vecSep)
      .setVecIdx(vecIdx)
      .setBatchSize(batchSize)
      .setQueryPartitionNum(queryPartitionNum)
      .setSheetsNum(sheetsNum)
      .setM(M)
      .setEf(ef)
      .setEfConstruction(efConstruction)
      .setMaxM(maxM)
      .setMaxM0(maxM0)
      .setML(mL)

    val ss = SparkSession.builder().getOrCreate()
    val df = DataUtils.loadVectors(ss, vectorPath, itemSep)

    val retDF = hnsw.transform(df)

    println(s"start to save")
    DataUtils.saveResult(retDF, outputPath, saveItemSep)

    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    // Add jvm parameters for executors
    if (!mode.startsWith("local")) {
      var executorJvmOptions = conf.get("spark.executor.extraJavaOptions")
      executorJvmOptions += " -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M "
      conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
      println(s"executorJvmOptions = ${executorJvmOptions}")
    }

    conf.setMaster(mode)
    conf.setAppName("HNSW")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
