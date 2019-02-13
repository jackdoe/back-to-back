package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
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

  public Consumer(List<String> addrs, Map<String, Worker> dispatch) throws IOException {
    Collections.shuffle(addrs);
    sem = new Semaphore(addrs.size());
    reconnect = new ArrayBlockingQueue<>(addrs.size());
    for (String addr : addrs) {
      HostAndPort hp = HostAndPort.fromString(addr);
      Broker b = new Broker(hp.getHost(), hp.getPort());
      start(b, dispatch);
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
