package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.types.Aim
import net.imagini.aim.tools.ColumnScanner
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.utils.ByteUtils

class LZ4Test  extends FlatSpec with Matchers {

  "BlockStorageLZ4" should "uncompress what it has compressed" in {
    val value1:String = 
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "\n"
            val value2:String = 
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

        val scanner = new ColumnScanner(instance, Aim.STRING)
        var ch:Char = 0
        var actualValue = "";
        while (!scanner.eof()) {
            ch = scanner.read.toChar
            actualValue += ch
        }
        value1+value2 should equal (actualValue)
  }
}