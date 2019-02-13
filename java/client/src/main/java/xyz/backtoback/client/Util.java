package xyz.backtoback.client;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class Util {
  private static final Logger logger = LoggerFactory.getLogger(Util.class);

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

  static SocketChannel connect(String h, int port, int retries) throws IOException {
    IOException last = null;
    for (int i = 0; i < retries; i++) {
      try {
        SocketChannel c = SocketChannel.open();
        c.connect(new InetSocketAddress(h, port));
        c.setOption(StandardSocketOptions.TCP_NODELAY, true);
        c.finishConnect();
        return c;
      } catch (IOException e) {
        last = e;
        logger.warn("failed to connect, retrying", e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ee) {
        }
      }
    }
    throw last;
  }
}
