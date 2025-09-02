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
package com.tencent.angel.graph.ann.index

import com.tencent.angel.graph.ann.{Item, ProgressListener, SearchResult}

trait IndexGraph extends Index with Serializable {
  
  val defaultProgressUpdateInterval: Int = 5000
  
  def add(item: Item): Boolean
  
  def addAll(items: Array[Item],
             listener: ProgressListener,
             progressUpdateInterval: Int = defaultProgressUpdateInterval): Unit = {
    
    val len = items.length
    for (idx <- items.indices) {
      this.add(items(idx))
      if (idx % progressUpdateInterval == 0 || idx == len - 1) {
        listener.updateProgress(idx + 1, len)
      }
    }
  }
  
  def size: Int
  
  def apply(id: Long): Item
  
  def get(id: Long): Option[Item]
  
  def contains(id: Long): Boolean
  
  def findNearest(vector: Array[Float], k: Int): Seq[SearchResult]
  
  def findNeighbors(id: Long, k: Int): Seq[SearchResult]
  
  def save(path: String): Boolean
  
  def load(path: String): Boolean
  
}
