package net.imagini.aim

import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.utils.View
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class BlockStorageTest extends FlatSpec with Matchers {
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

  val instance = new BlockStorageLZ4()
  instance.addBlock(ByteUtils.wrap((value1).getBytes))
  instance.addBlock(ByteUtils.wrap((value2).getBytes))
  instance.compressedSize should equal(168)
  instance.originalSize should equal(value1.length + value2.length)

  ByteUtils.compare(instance.view(0).array, instance.view(0).offset, instance.view(0).size, value1.getBytes, 0, value1.getBytes.length) should be(0)
  ByteUtils.compare(instance.view(1).array, instance.view(1).offset, instance.view(1).size, value2.getBytes, 0, value2.getBytes.length) should be(0)
}