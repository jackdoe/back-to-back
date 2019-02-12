package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import static xyz.backtoback.client.Util.receive;
import static xyz.backtoback.client.Util.send;

public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private SocketChannel channel;

  public Producer(SocketChannel channel) throws UnknownHostException, IOException {
    this.channel = channel;
    channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
  }

  public IO.Message produce(String topic, IO.Message message) throws IOException {
    send(channel, message.toBuilder().setTopic(topic).setTimeoutMs(1000).build());
    return receive(channel);
  }
}
