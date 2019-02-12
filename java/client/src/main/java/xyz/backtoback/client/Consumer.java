package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Map;

import static xyz.backtoback.client.Util.receive;
import static xyz.backtoback.client.Util.send;

public class Consumer {
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private SocketChannel channel;

  public Consumer(SocketChannel channel) throws UnknownHostException, IOException {
    this.channel = channel;
    channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
  }

  public void work(Map<String, Worker> dispatch) throws IOException, InterruptedException {
    IO.Poll poll = IO.Poll.newBuilder().addAllTopic(dispatch.keySet()).build();
    POLL:
    for (; ; ) {
      Thread.sleep(100);
      for (; ; ) {
        send(channel, poll);
        IO.Message m = receive(channel);
        if (m.getType().getNumber() == IO.MessageType.EMPTY.getNumber()) {
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
      }
    }
  }

  @FunctionalInterface
  public interface Worker {
    IO.Message process(IO.Message m);
  }
}
