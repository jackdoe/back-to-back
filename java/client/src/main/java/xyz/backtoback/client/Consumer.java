package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

public class Consumer {
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private ArrayBlockingQueue<Broker> reconnect;
  private Thread reconnector;
  private Semaphore sem;

  public Consumer(BrokerConf brokerConf, Map<String, Worker> dispatch) {
    this(brokerConf.poolSize(), brokerConf.asBrokerList(), dispatch);
  }

  private Consumer(int n, List<String> addrs, Map<String, Worker> dispatch) {
    Collections.shuffle(addrs);
    sem = new Semaphore(n);

    reconnect = new ArrayBlockingQueue<>(addrs.size());
    for (int i = 0; i < n; i++) {
      for (String addr : addrs) {
        HostAndPort hp = HostAndPort.fromString(addr);
        Broker b = new Broker(hp.getHost(), hp.getPort());
        start(b, dispatch);
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
                        start(b, dispatch);
                      });
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
    reconnector.start();
  }

  private void start(Broker b, Map<String, Worker> dispatch) {
    CompletableFuture.runAsync(
        () -> {
          try {
            b.consume(sem, dispatch);
          } catch (Exception e) {
            reconnect.add(b);
          }
        });
  }

  @FunctionalInterface
  public interface Worker {
    IO.Message process(IO.Message m);
  }
}
