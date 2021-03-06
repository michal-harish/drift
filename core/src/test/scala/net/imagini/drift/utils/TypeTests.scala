package net.imagini.drift.utils

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.nio.ByteBuffer
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.types.Drift
import java.net.InetAddress
import java.text.SimpleDateFormat

class TypeTests extends FlatSpec with Matchers {

  "DriftTypeBOOL" should "have a correct custom parser" in {
    val out = new Array[Byte](1)
    Drift.BOOL.parse(new View("true".getBytes()), out, 0)
    out(0) should be(1)
    Drift.BOOL.parse(new View("false".getBytes()), out, 0)
    out(0) should be(0)
    Drift.BOOL.parse(new View("0".getBytes()), out, 0)
    out(0) should be(0)
    Drift.BOOL.parse(new View("1".getBytes()), out, 0)
    out(0) should be(1)
    Drift.BOOL.parse(new View("9".getBytes()), out, 0)
    out(0) should be(1)
  }

  "DriftTypeBYTE" should "have a correct custom parser" in {
    val out = new Array[Byte](1)
    Drift.BYTE.parse(new View("0".getBytes()), out, 0)
    out(0) should be(0)
    Drift.BYTE.parse(new View("255".getBytes()), out, 0)
    out(0) & 0xFF should be(255)
    an[IllegalArgumentException] must be thrownBy Drift.BYTE.parse(new View("256".getBytes()), out, 0)
    Drift.BYTE.parse(new View("99".getBytes()), out, 0)
    out(0) should be(99)
    an[IllegalArgumentException] must be thrownBy Drift.BYTE.parse(new View("10xyz".getBytes()), out, 0)
  }

  "DriftTypeINT" should "have a correct custom parser" in {
    val out = new Array[Byte](4)
    Drift.INT.parse(new View("0".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(0)
    Drift.INT.parse(new View("255".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(255)
    Drift.INT.parse(new View(Integer.MAX_VALUE.toString.getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(Integer.MAX_VALUE)
    Drift.INT.parse(new View("-255".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(-255)
    Drift.INT.parse(new View("-65535".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(-65535)
    Drift.INT.parse(new View(Integer.MIN_VALUE.toString.getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(Integer.MIN_VALUE)
    an[IllegalArgumentException] must be thrownBy Drift.BYTE.parse(new View("10xyz".getBytes()), out, 0)
  }

  "DriftTypeLONG" should "have a correct custom parser" in {
    val out = new Array[Byte](8)
    Drift.LONG.parse(new View("0".getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(0)
    Drift.LONG.parse(new View("255".getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(255)
    Drift.LONG.parse(new View(Integer.MAX_VALUE.toString.getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(Integer.MAX_VALUE)
    Drift.LONG.parse(new View(Long.MaxValue.toString.getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(Long.MaxValue)
    an[IllegalArgumentException] must be thrownBy Drift.LONG.parse(new View("-255".getBytes()), out, 0)
    an[IllegalArgumentException] must be thrownBy Drift.LONG.parse(new View("-65535".getBytes()), out, 0)
    an[IllegalArgumentException] must be thrownBy Drift.LONG.parse(new View(Integer.MIN_VALUE.toString.getBytes()), out, 0)
    an[IllegalArgumentException] must be thrownBy Drift.LONG.parse(new View("10xyz".getBytes()), out, 0)
  }

  "DriftTypeSTRING" should "have a correct custom parser" in {
    val out = new Array[Byte](100)
    Drift.STRING.parse(new View("Hello World".getBytes()), out, 0)
    Drift.STRING.asString(out) should be("Hello World")
    Drift.STRING.parse(new View("".getBytes()), out, 0)
    Drift.STRING.asString(out) should be("")
  }

  "DriftTypeIPV4" should "have a correct custom parser" in {
    val out = new Array[Byte](4)
    Drift.IPV4.parse(new View("255.255.255.255".getBytes()), out, 0)
    InetAddress.getByAddress(out).getHostAddress() should be("255.255.255.255");
    Drift.IPV4.parse(new View("0.0.0.0".getBytes()), out, 0)
    InetAddress.getByAddress(out).getHostAddress() should be("0.0.0.0");
    Drift.IPV4.parse(new View("192.99.99.99".getBytes()), out, 0)
    InetAddress.getByAddress(out).getHostAddress() should be("192.99.99.99");
  }

  "DriftTypeUUID" should "have a correct custom parser" in {
    val out = new Array[Byte](16)
    Drift.UUID.parse(new View("37b22cfb-a29e-42c3-a3d9-12d32850e103".getBytes()), out, 0)
    Drift.UUID.asString(out) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103")
  }

  "DriftTypeTIME" should "have a correct custom parser" in {
    val formatter = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
    val out = new Array[Byte](8)
    Drift.TIME.parse(new View("1970-01-01 00:00:01".getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(1000);
    Drift.TIME.asString(out) should be("1970-01-01 00:00:01")
    Drift.TIME.parse(new View("2018-12-31 23:59:59".getBytes()), out, 0)
    Drift.TIME.asString(out) should be("2018-12-31 23:59:59")

    Drift.TIME.parse(new View("2014-10-10 16:00:01".getBytes()), out, 0)
    Drift.TIME.asString(out) should be("2014-10-10 16:00:01")

    Drift.TIME.parse(new View("1414768381000".getBytes()), out, 0)
    Drift.TIME.asString(out) should be("2014-10-31 15:13:01")

  }

  "converting schema types" should "convert each data type correctly" in {
    val schema = DriftSchema.fromString("uuid(UUID),str(STRING),int(INT),bool(BOOL),long(LONG),ip(IPV4),t(TIME)")
    val uuid = schema.get(0)
    val str = schema.get(1)
    val i = schema.get(2)
    val b = schema.get(3)
    val l = schema.get(4)
    val ip = schema.get(5)
    val t = schema.get(6)
    //val d = schema.get(7)

    val buf = ByteBuffer.allocate(1000)
    val r1 = new View(buf)
    val u1 = new View(buf)
    buf.put(uuid.convert("37b22cfb-a29e-42c3-a3d9-12d32850e103"))
    val s1 = new View(buf)
    buf.put(str.convert("abcd1234"))
    buf.put(i.convert("65536"))
    buf.put(b.convert("false"))
    buf.put(l.convert("123456789"))
    buf.put(ip.convert("192.168.0.1"))
    buf.put(t.convert("2014-12-31 23:59:59"))

    val r2 = new View(buf)
    val u2 = new View(buf)
    buf.put(uuid.convert("17b22cfb-a29e-42c3-a3d9-12d32850e103"))
    val s2 = new View(buf)
    buf.put(str.convert("xyz987"))
    buf.put(i.convert("65537"))
    buf.put(b.convert("false"))
    buf.put(l.convert("987654321"))
    buf.put(ip.convert("127.0.0.1"))
    buf.put(t.convert("2015-12-31 23:59:59"))

    schema.asString(r1, " ") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 abcd1234 65536 false 123456789 192.168.0.1 2014-12-31 23:59:59")
    schema.asString(r2, " ") should be("17b22cfb-a29e-42c3-a3d9-12d32850e103 xyz987 65537 false 987654321 127.0.0.1 2015-12-31 23:59:59")

    buf.flip
    buf.limit should be(16 + 4 + 8 + 4 + 16 + 4 + 6 + 4 + 1 + 1 + 8 + 8 + 4 + 4 + 8 + 8)

    uuid.asString(u1) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103")
    str.asString(s1) should be("abcd1234")
    uuid.asString(u2) should be("17b22cfb-a29e-42c3-a3d9-12d32850e103")
    str.asString(s2) should be("xyz987")
  }

}