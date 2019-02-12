package xyz.backtoback.client;

import com.google.protobuf.ByteString;
import xyz.backtoback.proto.IO;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < 10; i++) {
      new Thread(
              () -> {
                try {
                  consume();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })
          .start();
    }

    final AtomicLong n = new AtomicLong(0);

    for (int i = 0; i < 10; i++) {
      new Thread(
              () -> {
                try {
                  produce(n);
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

  public static void consume() throws Exception {
    SocketChannel sc = SocketChannel.open();
    sc.connect(new InetSocketAddress("localhost", 9001));
    Consumer c = new Consumer(sc);
    Map<String, Consumer.Worker> dispatch = new HashMap<>();
    dispatch.put(
        "abc",
        (m) -> {
          return IO.Message.newBuilder()
              .setData(ByteString.copyFrom("hello from consumer".getBytes()))
              .build();
        });

    c.work(dispatch);
  }

  public static void produce(AtomicLong n) throws Exception {
    SocketChannel sc = SocketChannel.open();
    sc.connect(new InetSocketAddress("localhost", 9000));
    Producer p = new Producer(sc);

    for (; ; ) {
      IO.Message m =
          p.produce(
              "abc",
              IO.Message.newBuilder()
                  .setData(ByteString.copyFrom("hello world".getBytes()))
                  .build());
      n.getAndIncrement();
    }
  }
}
