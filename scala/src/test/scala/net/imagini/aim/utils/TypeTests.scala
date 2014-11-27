package net.imagini.aim.utils

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.nio.ByteBuffer
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.Aim
import java.net.InetAddress
import java.text.SimpleDateFormat

class TypeTests extends FlatSpec with Matchers {

  "AimTypeBOOL" should "have a correct custom parser" in {
    val out = new Array[Byte](1)
    Aim.BOOL.parse(new View("true".getBytes()), out, 0)
    out(0) should be(1)
    Aim.BOOL.parse(new View("false".getBytes()), out, 0)
    out(0) should be(0)
    Aim.BOOL.parse(new View("0".getBytes()), out, 0)
    out(0) should be(0)
    Aim.BOOL.parse(new View("1".getBytes()), out, 0)
    out(0) should be(1)
    Aim.BOOL.parse(new View("9".getBytes()), out, 0)
    out(0) should be(1)
  }

  "AimTypeBYTE" should "have a correct custom parser" in {
    val out = new Array[Byte](1)
    Aim.BYTE.parse(new View("0".getBytes()), out, 0)
    out(0) should be(0)
    Aim.BYTE.parse(new View("255".getBytes()), out, 0)
    out(0) & 0xFF should be(255)
    an[IllegalArgumentException] must be thrownBy Aim.BYTE.parse(new View("256".getBytes()), out, 0)
    Aim.BYTE.parse(new View("99".getBytes()), out, 0)
    out(0) should be(99)
    an[IllegalArgumentException] must be thrownBy Aim.BYTE.parse(new View("10xyz".getBytes()), out, 0)
  }

  "AimTypeINT" should "have a correct custom parser" in {
    val out = new Array[Byte](4)
    Aim.INT.parse(new View("0".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(0)
    Aim.INT.parse(new View("255".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(255)
    Aim.INT.parse(new View(Integer.MAX_VALUE.toString.getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(Integer.MAX_VALUE)
    Aim.INT.parse(new View("-255".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(-255)
    Aim.INT.parse(new View("-65535".getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(-65535)
    Aim.INT.parse(new View(Integer.MIN_VALUE.toString.getBytes()), out, 0)
    ByteUtils.asIntValue(out) should be(Integer.MIN_VALUE)
    an[IllegalArgumentException] must be thrownBy Aim.BYTE.parse(new View("10xyz".getBytes()), out, 0)
  }

  "AimTypeLONG" should "have a correct custom parser" in {
    val out = new Array[Byte](8)
    Aim.LONG.parse(new View("0".getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(0)
    Aim.LONG.parse(new View("255".getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(255)
    Aim.LONG.parse(new View(Integer.MAX_VALUE.toString.getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(Integer.MAX_VALUE)
    Aim.LONG.parse(new View(Long.MaxValue.toString.getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(Long.MaxValue)
    an[IllegalArgumentException] must be thrownBy Aim.LONG.parse(new View("-255".getBytes()), out, 0)
    an[IllegalArgumentException] must be thrownBy Aim.LONG.parse(new View("-65535".getBytes()), out, 0)
    an[IllegalArgumentException] must be thrownBy Aim.LONG.parse(new View(Integer.MIN_VALUE.toString.getBytes()), out, 0)
    an[IllegalArgumentException] must be thrownBy Aim.LONG.parse(new View("10xyz".getBytes()), out, 0)
  }

  "AimTypeSTRING" should "have a correct custom parser" in {
    val out = new Array[Byte](100)
    Aim.STRING.parse(new View("Hello World".getBytes()), out, 0)
    Aim.STRING.convert(out) should be("Hello World")
    Aim.STRING.parse(new View("".getBytes()), out, 0)
    Aim.STRING.convert(out) should be("")
  }

  "AimTypeIPV4" should "have a correct custom parser" in {
    val out = new Array[Byte](4)
    Aim.IPV4.parse(new View("255.255.255.255".getBytes()), out, 0)
    InetAddress.getByAddress(out).getHostAddress() should be("255.255.255.255");
    Aim.IPV4.parse(new View("0.0.0.0".getBytes()), out, 0)
    InetAddress.getByAddress(out).getHostAddress() should be("0.0.0.0");
    Aim.IPV4.parse(new View("192.99.99.99".getBytes()), out, 0)
    InetAddress.getByAddress(out).getHostAddress() should be("192.99.99.99");
  }

  "AimTypeUUID" should "have a correct custom parser" in {
    val out = new Array[Byte](16)
    Aim.UUID.parse(new View("37b22cfb-a29e-42c3-a3d9-12d32850e103".getBytes()), out, 0)
    Aim.UUID.convert(out) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103")
  }

  "AimTypeTIME" should "have a correct custom parser" in {
    val formatter = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
    val out = new Array[Byte](8)
    Aim.TIME.parse(new View("1970-01-01 00:00:01".getBytes()), out, 0)
    ByteUtils.asLongValue(out) should be(1000);
    Aim.TIME.convert(out) should be("1970-01-01 00:00:01")
    Aim.TIME.parse(new View("2018-12-31 23:59:59".getBytes()), out, 0)
    Aim.TIME.convert(out) should be("2018-12-31 23:59:59")

    Aim.TIME.parse(new View("2014-10-10 16:00:01".getBytes()), out, 0)
    Aim.TIME.convert(out) should be("2014-10-10 16:00:01")

  }

  "converting schema types" should "convert each data type correctly" in {
    val schema = AimSchema.fromString("uuid(UUID),str(STRING),int(INT),bool(BOOL),long(LONG),ip(IPV4),t(TIME)")
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