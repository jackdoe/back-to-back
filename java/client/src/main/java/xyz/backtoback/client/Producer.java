package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  private final ExecutorService producePool;

  private final ArrayBlockingQueue<Broker> brokers;
  private final ArrayBlockingQueue<Broker> reconnect;
  private final List<Broker> allConnections;
  private final Thread reconnector;
  private final Thread expirator;

  public Producer(BrokerConf brokerConf) {
    List<String> addrs = brokerConf.asBrokerList();
    Collections.shuffle(addrs);

    this.producePool = Executors.newFixedThreadPool(brokerConf.poolSize());
    this.brokers = new ArrayBlockingQueue<>(addrs.size() * brokerConf.poolSize());
    this.reconnect = new ArrayBlockingQueue<>(addrs.size() * brokerConf.poolSize());
    this.allConnections = new ArrayList<>();
    for (int i = 0; i < brokerConf.poolSize(); i++) {
      for (String addr : addrs) {
        HostAndPort hp = HostAndPort.fromString(addr);
        Broker b = new Broker(hp.getHost(), hp.getPort());
        brokers.add(b);
        allConnections.add(b);
      }
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

    expirator =
        new Thread(
            () -> {
              while (true) {
                try {
                  Thread.sleep(1000);
                  for (Broker b : allConnections) {
                    if (b.expired()) {
                      logger.warn(
                          "disconnecting {} because it exceeded its expire time", b.toString());
                      b.close();
                    }
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
    expirator.start();
  }

  public IO.Message produce(String topic, IO.Message message) throws Exception {
    while (true) {
      Broker b = null;
      if (message.getTimeoutAfterMs() > 0) {
        b = brokers.poll(message.getTimeoutAfterMs(), TimeUnit.MILLISECONDS);
        if (b == null) {
          throw new TimeoutException("timed out picking a broker");
        }
      } else {
        b = brokers.take();
      }
      try {
        IO.Message reply = b.produce(topic, message.getTimeoutAfterMs(), message);
        brokers.add(b);
        return reply;
      } catch (Broker.BrokerErrorException e) {
        brokers.add(b);
        throw e;
      } catch (Exception e) {
        logger.warn("failed to produce, picking another broker", e);
        reconnect.add(b);
      }
    }
  }
}
