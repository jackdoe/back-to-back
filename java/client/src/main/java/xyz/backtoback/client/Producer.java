package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static xyz.backtoback.client.Util.*;

public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  static ExecutorService producePool = Executors.newCachedThreadPool();
  private SocketChannel channel;
  private String host;
  private int port;

  public Producer(String host, int port) throws IOException {
    this.host = host;
    this.port = port;
    reconnect();
  }

  public void reconnect() throws IOException {
    this.channel = connect(host, port, 1000000);
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

  public synchronized IO.Message produce(String topic, int timeoutMs, IO.Message message)
      throws Exception {
    return produce(topic, timeoutMs, true, message);
  }

  public synchronized IO.Message produce(
      String topic, int timeoutMs, boolean doReconnect, IO.Message message) throws Exception {
    while (true) {

      try {
        if (timeoutMs == 0) return io(topic, timeoutMs, message);

        return CompletableFuture.supplyAsync(
                () -> {
                  try {
                    return io(topic, timeoutMs, message);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                },
                producePool)
            .get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        channel.socket().close();
        logger.warn("failed to produce", e);
        if (doReconnect) {
          reconnect();
        } else {
          throw e;
        }
      }
    }
  }
}
