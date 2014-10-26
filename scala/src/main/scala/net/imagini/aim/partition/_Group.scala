package net.imagini.aim.partition

import net.imagini.aim.utils.ByteKey
import net.imagini.aim.types.AimSchema
import scala.collection.mutable.LinkedList
import net.imagini.aim.tools.Scanner

/**
 * Group represents all records associated by the same key
 */
class Group(val key:ByteKey) {

 //val columns: Array[Scanner]
 val records:LinkedList[Array[Array[Byte]]] = LinkedList()

 def addRecord(record: Array[Array[Byte]]) = records :+ record

 def satisfies(/*constraint*/): Boolean = {
   true
 }

}