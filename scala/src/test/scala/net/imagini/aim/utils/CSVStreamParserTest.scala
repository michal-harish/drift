package net.imagini.aim.utils

import java.io.ByteArrayInputStream
import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import java.util.zip.GZIPInputStream
import java.util.UUID

class CSVStreamParserTest extends FlatSpec with Matchers {

  "Example feed with white space" should "be parse as trimmed" in {
    val fixture = "\r abc\t123\n\rdef   \t 456\r\n"
    val in = new ByteArrayInputStream(fixture.getBytes)
    val parser = new CSVStreamParser(in, '\t')
    parser.nextValue should be("abc")
    parser.nextValue should be("123")
    parser.nextValue should be("def")
    parser.nextValue should be("456")
    an[EOFException] must be thrownBy (parser.nextValue)
  }

  "Parsing .gz file" should "should produce correct number of records" in {
    val fixture = this.getClass.getResourceAsStream("/net/imagini/aim/cluster/datasync_big.csv.gz")
    val parser = new CSVStreamParser(new GZIPInputStream(fixture), '\t')
    try {
      var c = 0
      while (true) {
        c += 1
        val uuid = UUID.fromString(parser.nextValueAsString)
        val timestamp = java.lang.Long.parseLong(parser.nextValueAsString)
        val column = parser.nextValueAsString
        val value = parser.nextValueAsString
      }
      c should be(139)
    } catch {
      case e: EOFException ⇒ {}
    }
  }
}