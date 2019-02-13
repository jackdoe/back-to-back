package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static xyz.backtoback.client.Util.*;

public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  private SocketChannel channel;
  private String host;
  private int port;

  public Producer(String host, int port) throws IOException {
    this.host = host;
    this.port = port;
    this.channel = connect(host, port, 1000000);
  }

  public synchronized IO.Message produce(String topic, IO.Message message) throws IOException {
    while (true) {
      try {
        send(
            channel,
            message
                .toBuilder()
                .setType(IO.MessageType.REQUEST)
                .setTopic(topic)
                .setTimeoutMs(1000)
                .build());
        return receive(channel);
      } catch (IOException e) {
        channel.socket().close();
        this.channel = connect(host, port, 1000000);
      }
    }
  }
}
