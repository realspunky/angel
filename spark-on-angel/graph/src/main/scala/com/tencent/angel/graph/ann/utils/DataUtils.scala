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
package com.tencent.angel.graph.ann.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DataUtils {
  private val DELIMITER = "delimiter"
  private val HEADER = "header"
  def loadFromDF(dataset: Dataset[_],
                 itemIdCol: String,
                 vectorCol: String,
                 vecSep: String,
                 partitionNum: Int): RDD[(Long, Array[Float])] = {
    val data = dataset.select(itemIdCol, vectorCol).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0).toLong, row.getString(1).split(vecSep).map(_.toFloat)))
      .repartition(partitionNum)

    data
  }

  def loadIdQueries(ss: SparkSession, path: String, partNum: Int, sep: String = " "): RDD[Long] = {
    val sc = ss.sparkContext

    val qIds = sc.textFile(path, partNum)
      .map(line => line.trim.split(sep))
      .flatMap(lis => lis.map(_.toLong))
      .repartition(partNum)

    qIds
  }

  def loadVectors(ss: SparkSession, path: String, itemSep: String = ":"): DataFrame = {
    val schema = StructType(Seq(
      StructField("item", StringType, nullable = false),
      StructField("vector", StringType, nullable = false)))

    ss.read
      .option("sep", itemSep)
      .option("header", "false")
      .schema(schema)
      .csv(path)
  }

  def saveResult(df: DataFrame, path: String, sep: String = "\t"): Unit = {
    df.printSchema()
    df.write
      .mode(SaveMode.Overwrite)
      .option(HEADER, "false")
      .option(DELIMITER, sep)
      .csv(path)

  }
}
