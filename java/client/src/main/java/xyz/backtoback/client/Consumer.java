package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Random;

import static xyz.backtoback.client.Util.receive;
import static xyz.backtoback.client.Util.send;

public class Consumer {
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private SocketChannel channel;

  public Consumer(String host, int port) throws IOException {
    SocketChannel c = SocketChannel.open();
    c.connect(new InetSocketAddress(host, port));

    this.channel = c;
    this.channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
  }

  public void work(Map<String, Worker> dispatch) throws IOException, InterruptedException {
    IO.Poll poll = IO.Poll.newBuilder().addAllTopic(dispatch.keySet()).build();
    int r = new Random(System.nanoTime()).nextInt(20);

    int maxSleep = 100 - r;
    int sleep = maxSleep;

    POLL:
    for (; ; ) {
      if (sleep > 0) Thread.sleep(sleep);
      for (; ; ) {
        send(channel, poll);
        IO.Message m = receive(channel);
        if (m.getType().getNumber() == IO.MessageType.EMPTY.getNumber()) {
          if (sleep < maxSleep) sleep++;
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
        if (sleep >= 5) {
          sleep -= 5;
        }
      }
    }
  }

  @FunctionalInterface
  public interface Worker {
    IO.Message process(IO.Message m);
  }
}
