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
package com.tencent.angel.graph.ann

import java.util.Objects

class SearchResult(private val item: Item,
                   private val distance: Float,
                   private val distanceComparator: Ordering[Float]) extends Ordered[SearchResult] with Serializable {
  
  def this(it: Item, dis: Float) = this(it, dis, Ordering.fromLessThan[Float]((a, b) => a < b))
  
  def getItem: Item = this.item
  def getDistance: Float = this.distance
  
  override def compare(that: SearchResult): Int = {
    this.distanceComparator.compare(this.distance, that.distance)
  }
  
  override def equals(obj: Any): Boolean = {
    if (obj == this) true
    else if (obj == null || obj.getClass != this.getClass) false
    else {
      val that = obj.asInstanceOf[SearchResult]
      if (Objects.equals(this.distance, that.distance) && Objects.equals(this.item, that.item)) true
      else false
    }
  }
  
  override def hashCode(): Int = {
    Objects.hash(this.distance.asInstanceOf[Object], this.item)
  }
  
  override def toString: String = {
    s"SearchResult{item: $item , distance: $distance }"
  }
  
}

object SearchResult {
  def apply(item: Item, distance: Float,
            distanceComparator: Ordering[Float]): SearchResult =
    new SearchResult(item, distance, distanceComparator)
  
  def unapply(result: SearchResult): Option[(Item, Float)] =
    Some(result.item, result.distance)
}
