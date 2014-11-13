package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.utils.BlockStorageFS
import net.imagini.aim.segment.SegmentScanner
import java.io.EOFException
import java.nio.file.Files
import java.io.File

class BlockStorageFSLZ4Test extends FlatSpec with Matchers {

  val f1 = new File("/var/lib/drift/drift-system-test-uid"); if (f1.exists()) f1.listFiles.map(file => file.delete)
  val f2 = new File("/var/lib/drift/drift-system-test-value"); if (f2.exists()) f2.listFiles.map(file => file.delete)

  val schema = AimSchema.fromString("uid(UUID:BYTEARRAY[16]),value(STRING)")
  val segment = new AimSegmentUnsorted(schema).init(classOf[BlockStorageFS],"drift-system-test")
  segment.appendRecord("95c54c2e-2542-4f5e-8914-47e669a9578f", "Hello")
  segment.appendRecord("95c54c2e-2542-4f5e-8914-47e669a9578f", "World")
  segment.close

  val scanner = new SegmentScanner("*", "*", segment)
  scanner.next should be(true); scanner.selectLine(" ") should be("95c54c2e-2542-4f5e-8914-47e669a9578f Hello")
  scanner.next should be(true); scanner.selectLine(" ") should be("95c54c2e-2542-4f5e-8914-47e669a9578f World")
  scanner.next should be(false); an[EOFException] must be thrownBy (scanner.selectLine(" "))
}