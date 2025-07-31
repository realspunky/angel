package com.tencent.angel.spark.ml.util

object Murmur3 {
  final val C1_32: Int = 0xcc9e2d51
  final val C2_32: Int = 0x1b873593
  final val R1_32: Int = 15
  final val R2_32: Int = 13
  final val M_32: Int = 5
  final val N_32: Int = 0xe6546b64
  
  final val DEFAULT_SEED: Int = 104729
  
  def hash32(data: Array[Byte]): Int = {
    hash32(data, 0, data.length, DEFAULT_SEED)
  }
  
  def hash32(data: Array[Byte], offset: Int, length: Int, seed: Int): Int = {
    var hash = seed
    val nblocks: Int = length >> 2
    
    for (i <- 0 until nblocks) {
      val i_4 = i << 2
      val k = (data(offset + i_4) & 0xff) |
        ((data(offset + i_4 + 1) & 0xff) << 8) |
        ((data(offset + i_4 + 2) & 0xff) << 16) |
        ((data(offset + i_4 + 3) & 0xff) << 24)
      
      hash = mix32(k, hash)
    }
    
    val idx = nblocks << 2
    var k1 = 0
    if (length - idx == 3) {
      k1 ^= data(offset + idx + 2) << 16
      k1 ^= data(offset + idx + 1) << 8
      k1 ^= data(offset + idx)
    }
    if (length - idx == 2) {
      k1 ^= data(offset + idx + 1) << 8
      k1 ^= data(offset + idx)
    }
    if (length - idx == 1) {
      k1 ^= data(offset + idx)
    }
    
    if (length - idx != 0) {
      k1 *= C1_32
      k1 = Integer.rotateLeft(k1, R1_32)
      k1 *= C2_32
      hash ^= k1
    }
    
    fmix32(length, hash)
  }
  
  def mix32(k: Int, hash: Int): Int = {
    var kt = k
    var hasht = hash
    kt *= C1_32
    kt = Integer.rotateLeft(kt, R1_32)
    kt *= C2_32
    hasht ^= k
    
    Integer.rotateLeft(hash, R2_32) * M_32 + N_32
  }
  
  def fmix32(length: Int, hash: Int): Int = {
    var hasht = hash
    hasht ^= length
    hasht ^= (hash >>> 16)
    hasht *= 0x85ebca6b
    hasht ^= (hash >>> 13)
    hasht *= 0xc2b2ae35
    hasht ^= (hash >>> 16)
    hasht
  }
  
}
