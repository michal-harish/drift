package net.imagini.aim.utils

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.nio.ByteBuffer
import net.imagini.aim.types.AimSchema

class TypeTests  extends FlatSpec with Matchers {

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

    schema.asString(r1," ") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 abcd1234 65536 false 123456789 192.168.0.1 2014-12-31 23:59:59")
    schema.asString(r2," ") should be("17b22cfb-a29e-42c3-a3d9-12d32850e103 xyz987 65537 false 987654321 127.0.0.1 2015-12-31 23:59:59")

    buf.flip
    buf.limit should be (16 + 4+8 + 4 + 16 + 4+6 + 4 + 1 + 1 + 8 + 8 + 4 + 4 + 8 + 8)

    uuid.asString(u1) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103")
    str.asString(s1) should be("abcd1234")
    uuid.asString(u2) should be("17b22cfb-a29e-42c3-a3d9-12d32850e103")
    str.asString(s2) should be("xyz987")

}