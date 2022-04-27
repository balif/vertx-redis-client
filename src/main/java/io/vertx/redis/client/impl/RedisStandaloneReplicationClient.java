package io.vertx.redis.client.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Response;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.vertx.redis.client.Command.INFO;
import static io.vertx.redis.client.Request.cmd;

public class RedisStandaloneReplicationClient extends BaseRedisClient implements Redis {

  private static final Logger LOG = LoggerFactory.getLogger(RedisStandaloneReplicationClient.class);

  private final RedisOptions options;

  private RedisReplicaEndpoints redisReplicaEndpoints;


  public RedisStandaloneReplicationClient(Vertx vertx, RedisOptions options) {
    super(vertx, options);
    this.options = options;
    discoverTopology()
      .onFailure(event -> LOG.error("Init connection to redis failed", event))
      .onSuccess(redisReplicaEndpoints -> {
        LOG.info("Init connection to redis end");
        this.redisReplicaEndpoints = redisReplicaEndpoints;
        LOG.info("Redis Master:" + this.redisReplicaEndpoints.getMaster());
        LOG.info("Redis Slaves:" + this.redisReplicaEndpoints.getSlaves());
      });

    vertx.setPeriodic(1000, event -> discoverTopology().onSuccess(rre -> {
      redisReplicaEndpoints = rre;
      LOG.trace("Redis Master:" + this.redisReplicaEndpoints.getMaster());
      LOG.trace("Redis Slaves:" + this.redisReplicaEndpoints.getSlaves());
    }));
  }

  @Override
  public Future<RedisConnection> connect() {
    final Promise<RedisConnection> promise = vertx.promise();
    if (redisReplicaEndpoints == null) {
      discoverTopology()
        .onFailure(event -> {
          promise.fail(event);
        })
        .onSuccess(redisReplicaEndpoints -> {
          this.redisReplicaEndpoints = redisReplicaEndpoints;
          getConnFromManager(redisReplicaEndpoints, promise);
        });
    } else {
      // Topology is know. Geting connectins from pool.
      getConnFromManager(redisReplicaEndpoints, promise);
    }
    return promise.future();
  }

  private void getConnFromManager(RedisReplicaEndpoints redisReplicaEndpoints, final Promise promise) {
    String m = redisReplicaEndpoints.getMaster();
    Future<TypeConnection> masterF = Future.succeededFuture();
    if (m != null) {
      masterF = connectionManager.getConnection(m, null)
        .map(redisConnection -> new TypeConnection(Type.MASTER, redisConnection))
        .recover(throwable -> {
          LOG.info("Connection to master failed " + throwable.getMessage());
          forceRediscovery();
          return Future.succeededFuture();
        });
    } else {
      forceRediscovery();
    }
    final Future<TypeConnection> finalMasterF = masterF;
    List<Future> connetionList = redisReplicaEndpoints.getSlaves().stream()
      .map(sl -> connectionManager.getConnection(sl, null)
        .map(redisConnection -> new TypeConnection(Type.SLAVE, redisConnection))
        .recover(throwable -> {
          LOG.info("Connection to slave failed. " + throwable.getMessage());
          forceRediscovery();
          return Future.succeededFuture();
        }))
      .collect(Collectors.toCollection(() -> {
        List<Future> l = new LinkedList();
        l.add(finalMasterF);
        return l;
      }));
    CompositeFuture.all(connetionList)
      .onSuccess(event -> {
        List<TypeConnection> connetions = event.list();
        RedisConnection master = null;
        List<RedisConnection> slaves = new LinkedList<>();
        for (TypeConnection tc : connetions) {
          if (tc != null && tc.connection != null) {
            switch (tc.type) {
              case MASTER:
                master = tc.connection;
                break;
              case SLAVE:
                slaves.add(tc.connection);
                break;
              default:
                throw new RuntimeException("Unsupported redis node type");
            }
          }
        }
        if (master == null && slaves.isEmpty()) {
          LOG.error("Missing connection to redis");
        }
        promise.handle(Future.succeededFuture(new RedisStandaloneReplicationConnection(vertx, options, master, slaves)));
      })
      .onFailure(event -> {
        LOG.error("getConnFromManager failed on CompositeFuture", event);
        promise.handle(Future.failedFuture(event));
      });
  }

  private void forceRediscovery() {
    LOG.info("Force rediscovery topology");
    discoverTopology().onSuccess(newRre -> {
      this.redisReplicaEndpoints = newRre;
      LOG.info("Redis Master:" + redisReplicaEndpoints.getMaster());
      LOG.info("Redis Slaves:" + redisReplicaEndpoints.getSlaves());
    });
  }

  private Future<RedisReplicaEndpoints> discoverTopology() {
    List<String> endpoints = options.getEndpoints();
    AtomicReference<EnpointConnection> master = new AtomicReference();
    List<EnpointConnection> slaveList = new ArrayList<>(endpoints.size());

    List<Future> connEndpoints = endpoints.stream().map(endpoint -> {
      final Promise promise = vertx.promise();
      Future<RedisConnection> futureConn = connectionManager.getConnection(endpoint, null);
      futureConn.onFailure(event -> {
        LOG.info("Discovery skiping endpoint:" + endpoint + " Message:" + event.getMessage());
        promise.complete();
      });
      futureConn.onSuccess(conn -> conn.send(cmd(INFO).arg("REPLICATION"), send -> {
        final Map<String, String> reply = parseInfo(send.result());
        EnpointConnection enpointConnection = new EnpointConnection(endpoint, conn);
        switch (reply.get("role")) {
          case "master":
            EnpointConnection prev = master.getAndSet(enpointConnection);
            if (prev != null) {
              LOG.error("Next master founded in endpoint list. Setting " + endpoint + " as master. " + " Prev master added as slave " + prev.getEndpoint());
              slaveList.add(prev);
            }
            break;
          case "slave":
            slaveList.add(enpointConnection);
            break;
        }
        promise.complete();
        conn.close();
      }));

      return promise.future();
    }).collect(Collectors.toList());

    final Promise promise = vertx.promise();
    CompositeFuture.all(connEndpoints).onComplete(event -> {
      String m = null;
      if (master.get() == null) {
        LOG.debug("Master not discovered");
      } else {
        m = master.get().getEndpoint();
      }
      List<String> sl = slaveList.stream().map(EnpointConnection::getEndpoint).collect(Collectors.toList());
      if (sl.isEmpty()) {
        LOG.debug("Replica not discovered");
      }
      if (m == null && sl.isEmpty()) {
        promise.handle(Future.failedFuture("Can not connect to any redis node."));
      } else {
        RedisReplicaEndpoints rre = new RedisReplicaEndpoints(m, sl);
        promise.handle(Future.succeededFuture(rre));
      }

    });
    return promise.future();
  }

  private Map<String, String> parseInfo(Response response) {
    if (response == null) {
      return Collections.emptyMap();
    }

    String text = response.toString(StandardCharsets.ISO_8859_1);
    if (text == null || text.length() == 0) {
      return Collections.emptyMap();
    }

    String[] lines = text.split("\r\n");
    Map<String, String> info = new HashMap<>();
    for (String line : lines) {
      int idx = line.indexOf(':');
      if (idx != -1) {
        info.put(line.substring(0, idx), line.substring(idx + 1));
      } else {
        info.put(line, null);
      }
    }
    return info;
  }

  private static class EnpointConnection {
    private final String endpoint;
    private final RedisConnection connection;

    EnpointConnection(String endpoint, RedisConnection connection) {
      this.endpoint = endpoint;
      this.connection = connection;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public RedisConnection getConnection() {
      return connection;
    }
  }

  private static class TypeConnection {
    private final Type type;
    private final RedisConnection connection;

    TypeConnection(Type type, RedisConnection connection) {
      this.type = type;
      this.connection = connection;
    }
  }

  private enum Type {
    MASTER, SLAVE
  }

  private static class RedisReplicaEndpoints {
    private final String master;
    private final List<String> slaves;

    RedisReplicaEndpoints(String master, List<String> slaves) {
      this.master = master;
      this.slaves = Collections.unmodifiableList(slaves);
    }

    public String getMaster() {
      return master;
    }

    public List<String> getSlaves() {
      return slaves;
    }
  }
}
