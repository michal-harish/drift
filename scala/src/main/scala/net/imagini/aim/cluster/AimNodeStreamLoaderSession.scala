package net.imagini.aim.cluster


/**
 * TODO Loader session should be able to recieve any stream
 * with content format specified in the header, this way we can directly pipe 
 * in a gzip file without having to decompress and parse in the loader and then pipe
 */
//class AimNodeStreamLoaderSession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {
//
//}