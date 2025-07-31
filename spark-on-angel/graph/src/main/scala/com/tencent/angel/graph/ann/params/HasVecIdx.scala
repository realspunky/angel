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
package com.tencent.angel.graph.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasVecIdx extends Params {
  /**
    * Param for vec index in tdw.
    *
    * @group param
    */
  final val vecIdx = new IntParam(this, "vecIdx", "param for vec index in tdw")
  
  /** @group getParam */
  final def getVecIdx: Int = $(vecIdx)
  
  setDefault(vecIdx, 0)
  
  /** @group setParam */
  final def setVecIdx(m: Int): this.type = set(vecIdx, m)
}