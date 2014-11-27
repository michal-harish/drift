package net.imagini.aim.utils

import java.io.ByteArrayInputStream
import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import java.util.zip.GZIPInputStream
import java.util.UUID
import net.imagini.aim.types.Aim

class CSVStreamParserTest extends FlatSpec with Matchers {
  val buf = new View(new Array[Byte](8192))

  "Example feed with white space" should "be parse as trimmed" in {
    val fixture = "\r abc\t123\n\rdef   \t 456\r\n"
    val in = new ByteArrayInputStream(fixture.getBytes)
    val parser = new CSVStreamParser(in, '\t')
    Aim.STRING.parse(parser.nextValue, buf.array, 0); Aim.STRING.asString(buf) should be("abc")
    Aim.STRING.parse(parser.nextValue, buf.array, 0); Aim.STRING.asString(buf) should be("123")
    Aim.STRING.parse(parser.nextValue, buf.array, 0); Aim.STRING.asString(buf) should be("def")
    Aim.STRING.parse(parser.nextValue, buf.array, 0); Aim.STRING.asString(buf) should be("456")
    an[EOFException] must be thrownBy (parser.nextValue)
  }

  "Parsing .gz file" should "should produce correct number of records" in {
    val fixture = this.getClass.getResourceAsStream("/net/imagini/aim/cluster/datasync_big.csv.gz")
    val parser = new CSVStreamParser(new GZIPInputStream(fixture), '\t')
    try {
      var c = 0
      while (true) {
        c += 1
        var o = 0
        o += Aim.UUID.parse(parser.nextValue, buf.array, o)
        o += Aim.LONG.parse(parser.nextValue, buf.array, o)
        o += Aim.STRING.parse(parser.nextValue, buf.array, o)
        o += Aim.STRING.parse(parser.nextValue, buf.array, o)
      }
      c should be(139)
    } catch {
      case e: EOFException ⇒ {}
    }
  }
}