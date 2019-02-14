package xyz.backtoback.client;

import com.google.protobuf.ByteString;
import xyz.backtoback.proto.IO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    List<String> addrs = new ArrayList<>();
    for (int i = 0; i < 9; i++) addrs.add("localhost:9001");

    Consumer c = new Consumer(addrs, dispatch);

    final AtomicLong n = new AtomicLong(0);

    List<String> brokers = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      brokers.add("localhost:9000");
    }
    Producer p = new Producer(brokers);
    for (int i = 0; i < 10; i++) {
      new Thread(
              () -> {
                try {
                  produce(p, n);
                } catch (Exception e) {
                  throw new RuntimeException(e);
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
      IO.Message m =
          p.produce(
              "abc",
              IO.Message.newBuilder()
                  .setTimeoutMs(10)
                  .setData(ByteString.copyFrom("hello world".getBytes()))
                  .build());
      n.getAndIncrement();
    }
  }
}
