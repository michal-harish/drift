package net.imagini.aim.utils

import java.nio.ByteBuffer

trait BlockStorage {

  def addBlock(block:ByteBuffer):Int
  def numBlocks:Int
  def compressedSize:Long
  def decompress(block:Int):ByteBuffer
  def createWriterBuffer:ByteBuffer
}