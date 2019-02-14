package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  static ExecutorService producePool = Executors.newCachedThreadPool();
  ArrayBlockingQueue<Broker> brokers;
  ArrayBlockingQueue<Broker> reconnect;
  Thread reconnector;

  public Producer(List<String> addrs) throws IOException {
    Collections.shuffle(addrs);

    brokers = new ArrayBlockingQueue<>(addrs.size());
    reconnect = new ArrayBlockingQueue<>(addrs.size());
    for (String addr : addrs) {
      HostAndPort hp = HostAndPort.fromString(addr);
      Broker b = new Broker(hp.getHost(), hp.getPort());
      brokers.add(b);
    }

    reconnector =
        new Thread(
            () -> {
              while (true) {
                try {
                  Broker b = reconnect.take();
                  logger.info("reconnecting {}", b);
                  CompletableFuture.runAsync(
                      () -> {
                        b.reconnect();
                        brokers.add(b);
                      });
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
    reconnector.start();
  }

  public IO.Message produce(String topic, IO.Message message) throws Exception {
    while (true) {
      Broker b = brokers.take();
      try {
        IO.Message reply = b.produce(topic, message.getTimeoutMs(), message);
        brokers.add(b);
        return reply;
      } catch (Broker.BrokerErrorException e) {
        logger.warn("failed to produce, retrying", e);
        brokers.add(b);
      } catch (Exception e) {
        logger.warn("failed to produce, picking another broker", e);
        reconnect.add(b);
      }
    }
  }
}
