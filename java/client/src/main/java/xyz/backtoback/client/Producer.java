package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

import static xyz.backtoback.client.Util.receive;
import static xyz.backtoback.client.Util.send;

public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private SocketChannel channel;

  public Producer(String host, int port) throws IOException {
    SocketChannel c = SocketChannel.open();
    c.connect(new InetSocketAddress(host, port));

    this.channel = c;
    this.channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
  }

  public synchronized IO.Message produce(String topic, IO.Message message) throws IOException {
    send(
        channel,
        message
            .toBuilder()
            .setType(IO.MessageType.REQUEST)
            .setTopic(topic)
            .setTimeoutMs(1000)
            .build());
    return receive(channel);
  }
}
