package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder

object AimNode extends App {

  var port = 4000
  var filename: String = null
  var separator = "\n"
  var gzip = false
  var schema: Option[AimSchema] = None
  val argsIterator = args.iterator
  var limit = 0L
  var segmentSize = 10485760
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--segment-size" ⇒ segmentSize = argsIterator.next.toInt
      case "--port"         ⇒ port = argsIterator.next.toInt
      case "--separator"    ⇒ separator = argsIterator.next
      case "--gzip"         ⇒ gzip = true;
      case "--schema"       ⇒ schema = Some(AimSchema.fromString(argsIterator.next))
      case "--filename"     ⇒ filename = argsIterator.next
      case arg: String      ⇒ println("Unknown argument " + arg)
    }
  }

  val key = schema.get.name(0)
  val sortOrder = SortOrder.ASC
  val partition = new AimPartition(schema.get, segmentSize, key, sortOrder)
  val server = new AimNode(partition, port).start

  new AimPartitionLoader("localhost", port, schema.get, separator, filename, gzip).processInput

}

class AimNode(partition: AimPartition, port: Int) {
//        this.partition = partition;
//        this.port = port;
//
//        controllerListener = new ServerSocket(port);
//        controllerAcceptor = new Thread() {
//            @Override public void run() {
//                System.out.println("Aim Partition Server accepting connections on port " + AimPartitionServer.this.port + " " + AimPartitionServer.this.partition.schema);
//                    while(true) {
//                        Socket socket;
//                        try {
//                            socket = controllerListener.accept();
//                            if (interrupted()) break;
//                        } catch (IOException e) {
//                            System.out.println(e.getMessage());
//                            break;
//                        }
//                        Pipe pipe;
//                        try {
//                            if (interrupted()) break;
//                            pipe = Pipe.open(socket);
//                            System.out.println(pipe.protocol + " connection from " + socket.getRemoteSocketAddress().toString());
//                            switch(pipe.protocol) {
//                                //TODO case BINARY: new ReaderThread(); break;
//                                case LOADER: new AimPartitionServerLoaderSession(AimPartitionServer.this.partition,pipe).start(); break;
//                                case QUERY: new AimPartitionServerQuerySession(AimPartitionServer.this.partition,pipe).start(); break;
//                                default: System.out.println("Unsupported protocol request " + pipe.protocol);
//                            }
//                        } catch (IOException e) {
//                            System.out.println("Aim Server at port "+AimPartitionServer.this.port +" failed to establish client connection: " + e.getMessage());
//                            continue;
//                        }
//                    }
//            }
//        };

  def start = {
//          controllerAcceptor.start();
//        try {
//            controllerAcceptor.join();
//        } catch (InterruptedException e1) {
//            controllerAcceptor.interrupt();
//        } finally {
//            try {
//                controllerListener.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            System.out.println("Aim Server at port "+AimPartitionServer.this.port + " shut down.");
//        }
  }
} 