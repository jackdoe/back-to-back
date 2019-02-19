package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  ArrayBlockingQueue<ConnectionProducer> free;

  public Producer(BrokerConf brokerConf) {
    free = new ArrayBlockingQueue<>(brokerConf.poolSize());

    for (int i = 0; i < brokerConf.poolSize(); i++) {
      ConnectionProducer c = new ConnectionProducer(brokerConf);
      free.add(c);
    }
  }

  public IO.Message produce(String topic, IO.Message message) throws Exception {
    ConnectionProducer p = null;
    if (message.getTimeoutAfterMs() > 0) {
      p = free.poll(message.getTimeoutAfterMs(), TimeUnit.MILLISECONDS);
      if (p == null) {
        throw new TimeoutException("timed out picking a broker");
      }
    } else {
      p = free.take();
    }

    try {
      IO.Message res = p.produce(topic, message);
      return res;
    } finally {
      free.add(p);
    }
  }

  private static class ConnectionProducer {
    ArrayBlockingQueue<Broker> brokers;
    ArrayBlockingQueue<Broker> reconnect;
    Thread reconnector;

    private ConnectionProducer(BrokerConf brokerConf) {
      this(brokerConf.asBrokerList());
    }

    private ConnectionProducer(List<String> addrs) {
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
}
