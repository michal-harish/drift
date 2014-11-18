package net.imagini.aim.utils

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.nio.ByteBuffer
import net.imagini.aim.types.AimSchema

class TypeTests  extends FlatSpec with Matchers {

    val schema = AimSchema.fromString("uuid(UUID:BYTEARRAY[16]),str(STRING),int(INT)")
    val uuid = schema.get(0)
    val str = schema.get(1)
    val i = schema.get(2)

    val buf = ByteBuffer.allocate(1000)
    val r1 = new View(buf)
    val u1 = new View(buf)
    buf.put(uuid.convert("37b22cfb-a29e-42c3-a3d9-12d32850e103"))
    val s1 = new View(buf)
    buf.put(str.convert("abcd1234"))
    buf.put(i.convert("65536"))


    val r2 = new View(buf)
    val u2 = new View(buf)
    buf.put(uuid.convert("17b22cfb-a29e-42c3-a3d9-12d32850e103"))
    val s2 = new View(buf)
    buf.put(str.convert("xyz987"))
    buf.put(i.convert("65537"))

    schema.asString(r1," ") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 abcd1234 65536")
    schema.asString(r2," ") should be("17b22cfb-a29e-42c3-a3d9-12d32850e103 xyz987 65537")

    buf.flip
    buf.limit should be (16 + 4+8 + 4 + 16 + 4+6 + 4)

    uuid.asString(u1) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103")
    str.asString(s1) should be("abcd1234")
    uuid.asString(u2) should be("17b22cfb-a29e-42c3-a3d9-12d32850e103")
    str.asString(s2) should be("xyz987")

}