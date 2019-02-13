package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
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

  public void consume(Semaphore sem, Map<String, Consumer.Worker> dispatch)
      throws InterruptedException, IOException {
    IO.Poll poll = IO.Poll.newBuilder().addAllTopic(dispatch.keySet()).build();
    int r = new Random(System.nanoTime()).nextInt(20);

    int maxSleep = 100 - r;
    int sleep = maxSleep;

    POLL:
    while (true) {
      if (sleep > maxSleep / 4) Thread.sleep(maxSleep);

      while (true) {
        sem.acquire();
        try {
          send(channel, poll);
          IO.Message m = receive(channel);
          if (m.getType().getNumber() == IO.MessageType.EMPTY.getNumber()) {
            if (sleep < maxSleep) sleep++;
            sem.release();
            continue POLL;
          }
          IO.Message reply =
              dispatch
                  .get(m.getTopic())
                  .process(m)
                  .toBuilder()
                  .setTopic(m.getTopic())
                  .setType(IO.MessageType.REPLY)
                  .build();
          send(channel, reply);

          sleep = 0;
        } catch (IOException e) {
          logger.warn("error consuming", e);
          sem.release();
          break POLL;
        } finally {
          sem.release();
        }
      }
    }
    throw new IOException("connection issue");
  }
}
