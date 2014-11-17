package net.imagini.aim

import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.utils.View
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.cluster.StreamUtils
import net.imagini.aim.utils.BlockStorageMEM
import java.io.File
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.utils.BlockStorageFS
import net.imagini.aim.segment.SegmentScanner
import java.io.EOFException

class BlockStorageTest extends FlatSpec with Matchers {
  "BlockStorageMEM" should "create valid input stream without memory overhead" in {
    val value1: String =
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "\n"
    val value2: String =
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "\n"

    val instance = new BlockStorageMEM() //LZ4()
    instance.addBlock(ByteUtils.wrap((value1).getBytes))
    instance.addBlock(ByteUtils.wrap((value2).getBytes))
    //  instance.storedSize should equal(168)
    instance.storedSize should equal(instance.originalSize)
    instance.originalSize should equal(value1.length + value2.length)

    val in0 = instance.toInputStream;
    val b0 = new Array[Byte](value1.getBytes.length)
    StreamUtils.read(in0, b0, 0, b0.length)
    val b1 = new Array[Byte](value1.getBytes.length)
    StreamUtils.read(in0, b1, 0, b1.length)
    in0.close

    ByteUtils.compare(b0, 0, b0.length, value1.getBytes, 0, value1.getBytes.length) should be(0)
    ByteUtils.compare(b1, 0, b1.length, value2.getBytes, 0, value2.getBytes.length) should be(0)
  }

  "BlockStorageMEMLZ4" should "create valid input stream wiht minimum overhead" in {
    val value1: String =
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "\n"
    val value2: String =
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
        "\n"

    val instance = new BlockStorageMEMLZ4()
    instance.addBlock(ByteUtils.wrap((value1).getBytes))
    instance.addBlock(ByteUtils.wrap((value2).getBytes))
    instance.storedSize should equal(168)
    instance.originalSize should equal(value1.length + value2.length)

    val in0 = instance.toInputStream
    val b0 = new Array[Byte](value1.getBytes.length)
    StreamUtils.read(in0, b0, 0, b0.length)
    val b1 = new Array[Byte](value1.getBytes.length)
    StreamUtils.read(in0, b1, 0, b1.length)
    in0.close

    ByteUtils.compare(b0, 0, b0.length, value1.getBytes, 0, value1.getBytes.length) should be(0)
    ByteUtils.compare(b1, 0, b1.length, value2.getBytes, 0, value2.getBytes.length) should be(0)
  }

  "BlockStorageFS" should "give input stream for scannig with little overhead" in {
    val f1 = new File("/var/lib/drift/drift-system-test-uid"); if (f1.exists()) f1.listFiles.map(file ⇒ file.delete)
    val f2 = new File("/var/lib/drift/drift-system-test-value"); if (f2.exists()) f2.listFiles.map(file ⇒ file.delete)

    val schema = AimSchema.fromString("uid(UUID:BYTEARRAY[16]),value(STRING)")
    val segment = new AimSegmentUnsorted(schema).init(classOf[BlockStorageFS], "drift-system-test")
    segment.appendRecord("95c54c2e-2542-4f5e-8914-47e669a9578f", "Hello")
    segment.appendRecord("95c54c2e-2542-4f5e-8914-47e669a9578f", "World")
    segment.close

    val uuidStream = segment.getBlockStorage(0).toInputStream;
    schema.get(0).convert(StreamUtils.read(uuidStream, schema.get(0).getDataType())) should be("95c54c2e-2542-4f5e-8914-47e669a9578f")
    schema.get(0).convert(StreamUtils.read(uuidStream, schema.get(0).getDataType())) should be("95c54c2e-2542-4f5e-8914-47e669a9578f")
    an[EOFException] must be thrownBy (StreamUtils.read(uuidStream, schema.get(0).getDataType()))
    uuidStream.close

    val valueStream = segment.getBlockStorage(1).toInputStream;
    schema.get(1).convert(StreamUtils.read(valueStream, schema.get(1).getDataType())) should be("Hello")
    schema.get(1).convert(StreamUtils.read(valueStream, schema.get(1).getDataType())) should be("World")
    an[EOFException] must be thrownBy (StreamUtils.read(valueStream, schema.get(1).getDataType()))
    valueStream.close

  }
}