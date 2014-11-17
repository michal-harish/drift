package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.types.RollingBufferView
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.types.Aim
import java.io.ByteArrayInputStream
import net.imagini.aim.cluster.StreamUtils

class RollingBufferViewTest extends FlatSpec with Matchers {

  "RollingBufferView" should "block on reads and writes" in {

    val n = 10000
    val input = new Array[Byte](n * 4)
    for (i ← (1 to n)) ByteUtils.putIntValue(i, input, (i - 1) * 4)
    for (i ← (1 to n)) ByteUtils.asIntValue(input, (i - 1) * 4) should be(i)
    val inputStream = new ByteArrayInputStream(input)

    val buf = new RollingBufferView(18, Aim.INT) 

    val writer = new Thread() {
      override def run = {
        for (i ← (1 to n)) {
          buf.select(inputStream)
          buf.keep
        }
        buf.markEof
      }
    }

    val reader = new Thread() {
      override def run = {
        for (i ← (1 to n)) {
          buf.next
          buf.aimType.asString(buf) should be(i.toString)
        }
      }
    }

    writer.start
    buf.eof should be (false)
    reader.start
    writer.join
    reader.join
    buf.eof should be (true)
  }

}