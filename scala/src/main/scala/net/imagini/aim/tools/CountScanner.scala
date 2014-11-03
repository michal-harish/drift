package net.imagini.aim.tools

trait CountScanner extends AbstractScanner{

  final def count: Long = {
    rewind
    var count: Long = 0
    while (next) {
      count += 1
    }
    count
  }
}