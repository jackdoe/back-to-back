package xyz.backtoback.client;

import com.google.protobuf.Message;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class Util {

  static IO.Message receive(SocketChannel channel) throws IOException {
    ByteBuffer header = ByteBuffer.wrap(new byte[4]).order(ByteOrder.LITTLE_ENDIAN);

    int n = channel.read(header);
    if (n < 0) throw new IOException("closed");
    header.position(0);

    ByteBuffer b = ByteBuffer.allocate(header.getInt());
    n = channel.read(b);
    if (n < 0) throw new IOException("closed");
    b.position(0);

    IO.Message m = IO.Message.parseFrom(b);
    //    logger.info("received {}", m.toString());

    return m;
  }

  static void send(SocketChannel channel, Message m) throws IOException {
    //    logger.info("sending {}", m.toString());
    byte[] data = m.toByteArray();
    ByteBuffer header = ByteBuffer.wrap(new byte[4]).order(ByteOrder.LITTLE_ENDIAN);
    header.putInt(data.length);
    header.position(0);

    ByteBuffer b = ByteBuffer.wrap(data);
    b.position(0);
    channel.write(new ByteBuffer[] {header, b});
  }
}
