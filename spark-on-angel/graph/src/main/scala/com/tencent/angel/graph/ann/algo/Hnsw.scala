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

import com.tencent.angel.graph.ann.ANNModel
import com.tencent.angel.graph.ann.index.HnswIndex
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import com.tencent.angel.graph.ann.params.{HasEf, HasEfConstruction, HasM, HasML, HasMaxM, HasMaxM0, _}
import com.tencent.angel.graph.ann.partition.HnswPartition

class Hnsw(override val uid: String) extends ANNModel[HnswPartition]
  with HasM with HasEfConstruction with HasEf with HasMaxM with HasMaxM0 with HasML {
  
  def this() = this(Identifiable.randomUID("HNSW"))
  
  def transform(dataset: Dataset[_]): DataFrame = {
    
    this.start(dataset)
    
    println(s"start make ann partitions")
    val indexBuilder = new HnswIndex.HnswIndexBuilder($(M), $(efConstruction), $(ef), $(maxM), $(maxM0), $(mL))
    indexBuilder.setDistanceFunction($(disFunc))
    
    println(s"start make ann partitions and build graph")
    val timeBuildIndex = System.currentTimeMillis()
    
    val indexedGraphParts = data.mapPartitionsWithIndex{ (idx, iter) =>
      Iterator.single(HnswPartition.apply(idx, iter, indexBuilder))
    }.map(_.buildGraph())
    indexedGraphParts.persist($(storageLevel))
    indexedGraphParts.foreachPartition(_ => Unit)
    println(s"build index cost: ${(System.currentTimeMillis() - timeBuildIndex) / 1000.0} s")
    
    this.setANNPartsWithSheets(indexedGraphParts)
    this.processANNParts()
    
    this.saveKNNResults()
  }
}

