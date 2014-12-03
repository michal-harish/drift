package net.imagini.drift.utils

import java.io.File

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import net.imagini.drift.segment.DriftSegment
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.types.DriftTableDescriptor
import net.imagini.drift.types.SortType
import net.imagini.drift.region.DriftRegion

import java.nio.ByteBuffer

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
    instance.store(ByteBuffer.wrap((value1).getBytes))
    instance.store(ByteBuffer.wrap((value2).getBytes))
    instance.getStoredSize() should be(value1.length + value2.length)

    val v1 = new View(value1.getBytes)
    val v2 = new View(value2.getBytes)

    val view = instance.toView;
    view.available(value1.length()) should be(true)
    view.compareTo(v1) should be(0)
    view.skip; view.available(value2.length()) should be(true)
    view.compareTo(v2) should be(0)
    view.skip; view.available(1) should be(false)
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
    instance.store(ByteBuffer.wrap((value1).getBytes))
    instance.store(ByteBuffer.wrap((value2).getBytes))
    instance.getStoredSize() should be(168)

    val v1 = new View(value1.getBytes)
    val v2 = new View(value2.getBytes)

    val view = instance.toView;
    view.available(value1.length()) should be(true)
    view.compareTo(v1) should be(0)
    view.skip; view.available(value2.length()) should be(true)
    view.compareTo(v2) should be(0)
    view.skip; view.available(1) should be(false)
  }

  "BlockStorageFS" should "create valid input stream wiht minimum overhead" in {
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

    val f1 = new File("/var/lib/drift/drift-system-test.lz"); if (f1.exists()) f1.delete
    val instance = new BlockStorageFS("drift-system-test")
    instance.store(ByteBuffer.wrap((value1).getBytes))
    instance.store(ByteBuffer.wrap((value2).getBytes))
    instance.getStoredSize() should be(168)

    val v1 = new View(value1.getBytes)
    val v2 = new View(value2.getBytes)

    val view = instance.toView;
    view.available(value1.length()) should be(true)
    view.compareTo(v1) should be(0)
    view.skip; view.available(value2.length()) should be(true)
    view.compareTo(v2) should be(0)
    view.skip; view.available(1) should be(false)
  }

  "BlockStorageFS" should "give input stream for scannig with little overhead" in {
    val f1 = new File("/var/lib/drift/drift-system-test-uid"); if (f1.exists()) f1.listFiles.map(file ⇒ file.delete)
    val f2 = new File("/var/lib/drift/drift-system-test-value"); if (f2.exists()) f2.listFiles.map(file ⇒ file.delete)

    val schema = DriftSchema.fromString("uid(UUID),value(STRING)")
    val descriptor = new DriftTableDescriptor(schema, 10000, classOf[BlockStorageFS], SortType.NO_SORT)
    val region = new DriftRegion("drift-system-test", descriptor)
    region.addTestRecords(
      Seq("95c54c2e-2542-4f5e-8914-47e669a9578f", "Hello"),
      Seq("95c54c2e-2542-4f5e-8914-47e669a9578f", "World"))
    region.compact
    val uuidView = region.segments(0).getBlockStorage(0).toView;
    uuidView.available(16) should be(true)
    schema.get(0).asString(uuidView) should be("95c54c2e-2542-4f5e-8914-47e669a9578f")
    uuidView.skip; uuidView.available(16) should be(true)
    schema.get(0).asString(uuidView) should be("95c54c2e-2542-4f5e-8914-47e669a9578f")
    uuidView.skip; uuidView.available(16) should be(false)

    val valueView = region.segments(0).getBlockStorage(1).toView;
    valueView.available(-1) should be(true)
    schema.get(1).asString(valueView) should be("Hello")
    valueView.skip; valueView.available(-1) should be(true)
    schema.get(1).asString(valueView) should be("World")
    valueView.skip; valueView.available(-1) should be(false)

  }
}