package xyz.backtoback.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Random;

import static xyz.backtoback.client.Util.*;

public class Consumer {
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private SocketChannel channel;
  private String host;
  private int port;

  public Consumer(String host, int port) throws IOException {
    this.host = host;
    this.port = port;
    this.channel = connect(host, port, 1000000);
  }

  public void work(Map<String, Worker> dispatch) throws IOException, InterruptedException {
    IO.Poll poll = IO.Poll.newBuilder().addAllTopic(dispatch.keySet()).build();
    int r = new Random(System.nanoTime()).nextInt(20);

    int maxSleep = 100 - r;
    int sleep = maxSleep;

    CONNECT:
    while (true) {
      POLL:
      while (true) {
        if (sleep == maxSleep) Thread.sleep(sleep);

        while (true) {
          try {
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

            // this is a bit aggressive
            // assume traffic up to next maxSleep
            sleep = 0;
          } catch (IOException e) {
            logger.warn("error consuming", e);
            break POLL;
          }
        }
      }
      channel.socket().close();
      this.channel = connect(this.host, this.port, 100000);
    }
  }

  @FunctionalInterface
  public interface Worker {
    IO.Message process(IO.Message m);
  }

  public static class Logistic {
    private double rate;
    private double[] weights;

    public Logistic(int n) {
      this.rate = 0.001;
      weights = new double[n];
    }

    private static double sigmoid(double z) {
      return 1.0 / (1.0 + Math.exp(-z));
    }

    public void train(int label, int[] x) {
      double predicted = classify(x);
      for (int j = 0; j < weights.length; j++) {
        weights[j] = weights[j] + rate * (label - predicted) * x[j];
      }
    }

    private double classify(int[] x) {
      double logit = .0;
      for (int i = 0; i < weights.length; i++) {
        logit += weights[i] * x[i];
      }
      return sigmoid(logit);
    }
  }
}
