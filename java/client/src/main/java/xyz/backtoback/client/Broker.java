package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import static xyz.backtoback.client.Util.*;

class Broker {
  private static final Logger logger = LoggerFactory.getLogger(Broker.class);
  static ExecutorService producePool = Executors.newCachedThreadPool();
  private String addr;
  private int port;
  private SocketChannel channel;

  public Broker(String addr, int port) {
    this.addr = addr;
    this.port = port;
    this.channel = connect(addr, port);
  }

  public void reconnect() {
    logger.info("reconnecting {}", this.toString());
    try {
      channel.socket().close();
    } catch (Exception e) {
      logger.warn("failed to close,e");
    }
    this.channel = connect(addr, port);
  }

  @Override
  public String toString() {
    return String.format("%s:%d", addr, port);
  }

  private IO.Message io(String topic, int timeoutMs, IO.Message message)
      throws IOException, BrokerErrorException {
    send(
        channel,
        message
            .toBuilder()
            .setType(IO.MessageType.REQUEST)
            .setTopic(topic)
            .setTimeoutAfterMs(timeoutMs)
            .build());
    IO.Message m = receive(channel);
    if (m.getType().getNumber() != IO.MessageType.REPLY.getNumber()) {
      throw new BrokerErrorException(m.getType().toString());
    }
    return m;
  }

  public IO.Message produce(String topic, int timeoutMs, IO.Message message)
      throws InterruptedException, ExecutionException, TimeoutException, IOException,
          BrokerErrorException {
    if (timeoutMs == 0) return io(topic, timeoutMs, message);

    try {
      // assume the broker will notify us on timeout, so we expect the ERROR message
      return CompletableFuture.supplyAsync(
              () -> {
                try {
                  return io(topic, timeoutMs, message);
                } catch (BrokerErrorException e) {
                  throw e;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              },
              producePool)
          .get(timeoutMs + 1000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable clause = e.getCause();
      if (clause != null) {
        if (clause instanceof BrokerErrorException) {
          throw (BrokerErrorException) clause;
        }
      }
      throw e;
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
                  .setUuid(m.getUuid())
                  .setType(IO.MessageType.REPLY)
                  .build();
          send(channel, reply);

          sleep = 0;
        } catch (IOException e) {
          logger.warn("error consuming", e);
          break POLL;
        } finally {
          sem.release();
        }
      }
    }
    throw new IOException("connection issue");
  }

  public static class BrokerErrorException extends RuntimeException {
    BrokerErrorException(String s) {
      super(s);
    }
  }
}
