package xyz.backtoback.client;

/* copied from https://github.com/google/guava/blob/master/guava/src/com/google/common/net/HostAndPort.java */
public final class HostAndPort {
  private static final int NO_PORT = -1;
  private final String host;
  private final int port;
  private final boolean hasBracketlessColons;

  private HostAndPort(String host, int port, boolean hasBracketlessColons) {
    this.host = host;
    this.port = port;
    this.hasBracketlessColons = hasBracketlessColons;
  }

  public static HostAndPort fromParts(String host, int port) {
    HostAndPort parsedHost = fromString(host);
    return new HostAndPort(parsedHost.host, port, parsedHost.hasBracketlessColons);
  }

  public static HostAndPort fromHost(String host) {
    HostAndPort parsedHost = fromString(host);
    return parsedHost;
  }

  public static HostAndPort fromString(String hostPortString) {
    String host;
    String portString = null;
    boolean hasBracketlessColons = false;

    if (hostPortString.startsWith("[")) {
      String[] hostAndPort = getHostAndPortFromBracketedHost(hostPortString);
      host = hostAndPort[0];
      portString = hostAndPort[1];
    } else {
      int colonPos = hostPortString.indexOf(':');
      if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
        // Exactly 1 colon. Split into host:port.
        host = hostPortString.substring(0, colonPos);
        portString = hostPortString.substring(colonPos + 1);
      } else {
        // 0 or 2+ colons. Bare hostname or IPv6 literal.
        host = hostPortString;
        hasBracketlessColons = (colonPos >= 0);
      }
    }

    int port = NO_PORT;
    if (portString != null && !portString.equals("")) {
      try {
        port = Integer.parseInt(portString);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Unparseable port number: " + hostPortString);
      }
    }

    return new HostAndPort(host, port, hasBracketlessColons);
  }

  private static String[] getHostAndPortFromBracketedHost(String hostPortString) {
    int closeBracketIndex = 0;
    closeBracketIndex = hostPortString.lastIndexOf(']');
    String host = hostPortString.substring(1, closeBracketIndex);
    if (closeBracketIndex + 1 == hostPortString.length()) {
      return new String[] {host, ""};
    } else {
      return new String[] {host, hostPortString.substring(closeBracketIndex + 2)};
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }
}
