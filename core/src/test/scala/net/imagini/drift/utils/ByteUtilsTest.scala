package net.imagini.drift.utils

import org.scalatest.Matchers
import org.scalatest.FlatSpec

import java.nio.ByteBuffer
import java.util.TreeMap

class ByteUtilsTest extends FlatSpec with Matchers {

  "ByteKeys with same value and different classifiers" should "be different" in {
        val tree = new TreeMap[ByteKey,String]()
        val key2 = new ByteKey("12345678".getBytes(), 0, 0 ,2) 
        val key1 = new ByteKey("12345678".getBytes(), 0, 0 ,1)
        tree.put(key2, "two")
        tree.put(key1, "one")
        tree.size should equal(2)
        tree.pollFirstEntry.getValue should equal("one")
        tree.pollFirstEntry.getValue should equal("two")
    }

    "ByteBuffer slice " should "share the same memory" in {
        val bb1 = ByteBuffer.allocate(12)
        println("BB1 Position="+bb1.position+", " + "Limit="+bb1.limit+", " + "Capacity="+ bb1.capacity +", ")
        bb1.put("012345".getBytes)
        println("BB1 Position="+bb1.position+", " + "Limit="+bb1.limit+", " + "Capacity="+ bb1.capacity +", ")
        val bb2 = bb1.slice
        bb1.mark
        println("BB2 Position="+bb2.position+", " + "Limit="+bb2.limit+", " + "Capacity="+ bb2.capacity +", ")
        bb1.put("6789".getBytes)
        println("BB1 Position="+bb1.position+", " + "Limit="+bb1.limit+", " + "Capacity="+ bb1.capacity +", ")
        bb1.reset
        bb1.put("ABCDEF".getBytes)
        println("BB2 Position="+bb2.position+", " + "Limit="+bb2.limit+", " + "Capacity="+ bb2.capacity +", ")
        val s = new Array[Byte](4)
        bb2.get(s)
        val subsection = new String(s)
        println(subsection)
        println("BB2 Position="+bb2.position+", " + "Limit="+bb2.limit+", " + "Capacity="+ bb2.capacity +", ")
        subsection should equal("ABCD")
    }

    "Contains and compare" should " always work around the edges" in {
      val container = new View("123456789".getBytes)
      val subcontainer = new View(container.array, 4, 9,9)
      ByteUtils.equals(container.array, container.offset, container.size , "123456789".getBytes, 0, 9) should be (true)
      ByteUtils.equals(subcontainer.array, subcontainer.offset, 5, "56789".getBytes, 0, 5) should be (true)
      ByteUtils.contains(container.array, container.offset , 9, subcontainer.array, subcontainer.offset, 5) should be (true)
    }
}
