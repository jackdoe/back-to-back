package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static xyz.backtoback.client.Util.*;

class Broker {
  private static final Logger logger = LoggerFactory.getLogger(Broker.class);
  private String addr;
  private int port;
  private SocketChannel channel;

  public Broker(String addr, int port) {
    this.addr = addr;
    this.port = port;
    this.channel = connect(addr, port);
  }

  public void reconnect() {
    try {
      if (channel.isConnected()) channel.socket().close();
    } catch (Exception e) {
      logger.warn("failed to close,e");
    }
    this.channel = connect(addr, port);
  }

  @Override
  public String toString() {
    return String.format("%s:%d", addr, port);
  }

  private IO.Message io(String topic, int timeoutMs, IO.Message message) throws IOException {
    send(
        channel,
        message
            .toBuilder()
            .setType(IO.MessageType.REQUEST)
            .setTopic(topic)
            .setTimeoutMs(timeoutMs)
            .build());
    return receive(channel);
  }

  public IO.Message produce(String topic, int timeoutMs, IO.Message message) throws Exception {
    while (true) {
      if (timeoutMs == 0) return io(topic, timeoutMs, message);

      return CompletableFuture.supplyAsync(
              () -> {
                try {
                  return io(topic, timeoutMs, message);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              },
              Producer.producePool)
          .get(timeoutMs, TimeUnit.MILLISECONDS);
    }
  }
}
