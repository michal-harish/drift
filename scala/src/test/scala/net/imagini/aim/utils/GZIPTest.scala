package net.imagini.aim.utils

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import java.util.zip.GZIPInputStream

class GZIPTest  extends FlatSpec with Matchers {
  val testString = "SomeStringABCDEFGHIJKLMNOPQRSTUVWXYZSomeStringABCDEFGHIJKLMNOPQRSTUVWXYZSomeStringABCDEFGHIJKLMNOPQRSTUVWXYZSomeStringABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val byteStream = new ByteArrayOutputStream
  val out = new GZIPOutputStream(byteStream)
  out.write(testString.getBytes)
  out.finish
  val in = new GZIPInputStream(new ByteArrayInputStream(byteStream.toByteArray))
  val inBytes = new Array[Byte](testString.length)
  val x= in.read(inBytes)
  new String(inBytes) should equal(testString)
}