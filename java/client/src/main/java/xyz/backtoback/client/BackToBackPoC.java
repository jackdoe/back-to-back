package xyz.backtoback.client;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.protobuf.ByteString;
import xyz.backtoback.proto.IO;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BackToBackPoC {
  // static int THREADS = 12;
  static int THREADS = 4;
  static ExecutorService testPool;
  static ExecutorService producerPool;
  static ExecutorService consumerPool;
  static Map<Integer, AtomicInteger> counters =
      IntStream.range(0, THREADS)
          .boxed()
          .collect(Collectors.toMap(Function.identity(), k -> new AtomicInteger()));

  public static void main(String[] args) throws Exception {

    testPool = Executors.newFixedThreadPool(THREADS);
    producerPool = Executors.newFixedThreadPool(THREADS);
    consumerPool = Executors.newFixedThreadPool(THREADS);

    Map<String, Consumer.Worker> dispatch = new HashMap<>();
    dispatch.put(
        "abc",
        (m) -> {
          return IO.Message.newBuilder()
              .setData(ByteString.copyFrom("hello from consumer".getBytes()))
              .build();
        });

    for (int i = 0; i < 10; i++) {
      consumerPool.submit(
          () -> {
            try {
              Consumer consumer = new Consumer("localhost", 9001);
              consumer.work(dispatch);
            } catch (IOException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    }
    String request = "this is the request";

    System.out.println("done");
    //      int n = 10;
    //      int n = 100;
    //      int n = 1000;
    //      int n = 10_000;
    int n = 1000_000;
    //      int n = 1_000_000;
    //    int n = 100_000_000;

    //      for (int i = 0; i < 40000; i++) {

    List<Producer> producers = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      producers.add(new Producer("localhost", 9000));
    }

    List<CompletableFuture<Boolean>> wait = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      Producer producer = producers.get(i % producers.size());

      try {
        if (i % 1000 == 0) System.out.println("before: " + i);
        wait.add(
            CompletableFuture.supplyAsync(
                () -> {
                  try {
                    IO.Message m =
                        producer.produce(
                            "abc",
                            IO.Message.newBuilder()
                                .setData(ByteString.copyFrom("hello world".getBytes()))
                                .build());
                  } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                  return true;
                }));
      } catch (Exception e) {
        System.out.println("here " + i);
        e.printStackTrace();
      }
    }
    System.out.println("waiting");
    for (CompletableFuture<Boolean> booleanCompletableFuture : wait) {
      booleanCompletableFuture.get();
    }
    System.out.println("warmed up...");
    Multiset<Long> counts = ConcurrentHashMultiset.create();

    List<Future<IO.Message>> fList = new ArrayList<>();
    long tot = System.currentTimeMillis();

    for (int i = 0; i < n; i++) {

      if (i % 5000 == 0) System.out.println("i: " + i);
      Producer producer = producers.get(i % producers.size());

      Callable<IO.Message> c =
          () -> {
            try {
              long took = System.nanoTime();
              IO.Message message =
                  producer.produce(
                      "abc",
                      IO.Message.newBuilder()
                          .setData(ByteString.copyFrom("hello world".getBytes()))
                          .build());

              took = System.nanoTime() - took;
              counts.add(took / 1_000_000);
              return message;
            } catch (Exception e) {
              e.printStackTrace();
              throw e;
            }
          };
      Future<IO.Message> f = testPool.submit(c);
      fList.add(f);
    }
    System.out.println("DONE");
    for (ExecutorService pool : Arrays.asList(testPool)) { // , consumerPool, producerPool)) {
      System.out.println("shutting down");
      pool.shutdown();
      pool.awaitTermination(1, TimeUnit.MINUTES);
    }
    System.out.println("pools shutdown");

    int successCount = 0;
    for (Future<IO.Message> future : fList) {
      try {
        String r = future.get(1, TimeUnit.MINUTES).getData().toString(StandardCharsets.UTF_8);
        if (r != null && !r.isEmpty()) successCount++;
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(2);
      }
    }
    tot = System.currentTimeMillis() - tot;

    System.out.println("SUCCESS COUNT " + successCount + " took " + (tot));
    double throughput = n / (tot / 1000d);
    System.out.println(new DecimalFormat("###.##").format(throughput));

    counts.elementSet().stream()
        .sorted()
        .forEach(e -> System.out.println(String.format("%s >> %s", e, counts.count(e))));

    System.out.println("handled counters");
    counters.forEach((k, v) -> System.out.println(String.format("%s >> %s", k, v.get())));
    System.out.println(
        "tot consumed " + counters.values().stream().map(AtomicInteger::get).reduce(Integer::sum));
  }
}
