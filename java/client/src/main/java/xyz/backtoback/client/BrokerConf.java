package xyz.backtoback.client;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BrokerConf {
  private final Set<String> hosts;
  private final int poolSize;

  public BrokerConf(int poolSize, String... hosts) {
    this.hosts = Sets.newHashSet(hosts);
    this.poolSize = poolSize;
  }

  public Set<String> hosts() {
    return this.hosts;
  }

  public int poolSize() {
    return this.poolSize;
  }

  public List<String> asBrokerList() {
    List<String> addr = new ArrayList<>();
    for (String host : this.hosts()) {
      addr.add(host);
    }
    return addr;
  }
}
