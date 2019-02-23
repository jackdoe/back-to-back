package xyz.backtoback.client;

import com.google.protobuf.ByteString;
import xyz.backtoback.proto.IO;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

  public static void main(String[] args) throws Exception {
    Map<String, Consumer.Worker> dispatch = new HashMap<>();
    dispatch.put(
        "abc",
        (m) -> {
          return IO.Message.newBuilder()
              .setData(ByteString.copyFrom("hello from consumer".getBytes()))
              .build();
        });
    int thr = 10;
    Consumer c = new Consumer(new BrokerConf(thr, "localhost:9001"), dispatch);

    final AtomicLong n = new AtomicLong(0);

    Producer p = new Producer(new BrokerConf(thr, "localhost:9000"));
    for (int i = 0; i < 10; i++) {
      new Thread(
              () -> {
                try {
                  produce(p, n);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              })
          .start();
    }

    while (true) {
      System.out.println(String.format("%d per second", n.get()));
      n.set(0);
      Thread.sleep(1000);
    }
  }

  public static void produce(Producer p, AtomicLong n) throws Exception {
    for (; ; ) {
      try {
        IO.Message m =
            p.produce(
                "abc",
                IO.Message.newBuilder()
                    .setTimeoutAfterMs(0)
                    .setData(ByteString.copyFrom("hello world".getBytes()))
                    .build());
      } catch (Exception e) {
        e.printStackTrace();
      }
      n.getAndIncrement();
    }
  }
}
