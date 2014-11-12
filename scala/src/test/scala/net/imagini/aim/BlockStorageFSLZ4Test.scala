package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.utils.BlockStorageFSLZ4

class BlockStorageFSLZ4Test extends FlatSpec with Matchers {

//  val schema = AimSchema.fromString("uid(UUID:BYTEARRAY[16]),value(STRING)")
//  val segment = new AimSegmentUnsorted(schema, classOf[BlockStorageFSLZ4])
//  segment.appendRecord("95c54c2e-2542-4f5e-8914-47e669a9578f","Hello")
//  segment.close
}