package io.vertx.spi.cluster.impl.zookeeper;

import io.vertx.core.*;
import io.vertx.core.shareddata.AsyncMap;
import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Optional;

/**
 * Created by Stream.Liu
 */
class ZKAsyncMap<K, V> extends ZKMap<K, V> implements AsyncMap<K, V> {

  private final PathChildrenCache curatorCache;
  private final RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 5);
  private final EnsurePath ensurePath;

  ZKAsyncMap(Vertx vertx, CuratorFramework curator, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MAP, mapName);
    curatorCache = new PathChildrenCache(curator, mapPath, true);
    try {
      curatorCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      throw new VertxException(e);
    }
    ensurePath = curator.newNamespaceAwareEnsurePath(mapPath);
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    if (!keyIsNull(k, asyncResultHandler)) {
      checkExists(k, checkResult -> {
        if (checkResult.succeeded()) {
          if (checkResult.result()) {
            Optional.ofNullable(curatorCache.getCurrentData(keyPath(k)))
                .flatMap(childData -> Optional.of(childData.getData()))
                .ifPresent(data -> {
                  try {
                    V value = asObject(data);
                    vertx.runOnContext(handler -> asyncResultHandler.handle(Future.succeededFuture(value)));
                  } catch (Exception e) {
                    vertx.runOnContext(handler -> asyncResultHandler.handle(Future.failedFuture(e)));
                  }
                });
          } else {
            //ignore
            vertx.runOnContext(handler -> asyncResultHandler.handle(Future.succeededFuture()));
          }
        } else {
          vertx.runOnContext(handler -> asyncResultHandler.handle(Future.failedFuture(checkResult.cause())));
        }
      });
    }
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    if (!keyIsNull(k, completionHandler) && !valueIsNull(v, completionHandler)) {
      checkExists(k, existEvent -> {
        if (existEvent.succeeded()) {
          if (existEvent.result()) {
            setData(k, v, setDataEvent -> forwardAsyncResult(completionHandler, setDataEvent));
          } else {
            create(k, v, completionHandler);
          }
        } else {
          vertx.runOnContext(event -> completionHandler.handle(Future.failedFuture(existEvent.cause())));
        }
      });
    }
  }

  @Override
  public void put(K k, V v, long timeout, Handler<AsyncResult<Void>> completionHandler) {
    //TODO add note to the doc.
    //we don't need timeout since zookeeper only care session timeout which could be setting in zookeeper.properties
    put(k, v, completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
    if (!keyIsNull(k, completionHandler) && !valueIsNull(v, completionHandler)) {
      vertx.executeBlocking(future -> {
        putIfAbsent(k, v, future);
      }, completionHandler);
    }
  }

  @Override
  public void putIfAbsent(K k, V v, long timeout, Handler<AsyncResult<V>> completionHandler) {
    putIfAbsent(k, v, completionHandler);
  }

  /**
   * Recursively attempts to write a value to a znode using optimistic locking.
   */
  private void putIfAbsent(K k, V v, Future<V> future) {
    // Calculate the path to the znode for the key.
    String path = keyPath(k);

    // Serialize the value once.
    byte[] valueBytes;
    try {
      valueBytes = asByte(v);
    } catch (Exception e) {
      future.fail(e);
      return;
    }

    // Record the start time and retry count. These will be used to perform exponential backoff.
    long startTime = System.currentTimeMillis();
    int retries = 0;

    // Recursively attempt to set the value using a simple optimistic locking strategy.
    while (true) {
      Stat stat = new Stat();

      byte[] value = null;
      boolean create = false;

      // Ensure that the parent node exists in ZooKeeper, and get the current value for this node.
      try {
        ensurePath.ensure(curator.getZookeeperClient());
        value = curator.getData().storingStatIn(stat).forPath(path);
      } catch (KeeperException.NoNodeException e) {
        create = true;
      } catch (Exception e) {
        future.fail(e);
        return;
      }

      // If the node does not already exist, create it. This is necessary in order to retrieve a version from the path.
      if (create) {
        try {
          curator.create().forPath(path, asByte(null));
        } catch (KeeperException.NodeExistsException e) {
          // Do nothing useful.
        } catch (Exception e) {
          future.fail(e);
          return;
        }
      } else {
        // If the node already exists...
        try {
          // If the current value is null, attempt to set the value, ensuring the version has not changed since we
          // last read the node.
          V currentValue = asObject(value);
          if (currentValue == null) {
            try {
              curator.setData().withVersion(stat.getVersion()).forPath(path, valueBytes);
              future.complete(null);
              return;
            } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
              // If the version has changed, block on the retry policy if necessary. If no more retries are remaining,
              // fail the operation.
              if (!retryPolicy.allowRetry(retries++, System.currentTimeMillis() - startTime, RetryLoop.getDefaultRetrySleeper())) {
                future.fail("failed to acquire optimistic lock");
                return;
              }
            } catch (Exception e) {
              future.fail(e);
              return;
            }
          } else {
            // If the value is not null then simply return the null value. This is atomic since we had no operation to
            // perform in the event that the value was not null.
            try {
              future.complete(currentValue);
            } catch (Exception e) {
              future.fail(e);
            }
            return;
          }
        } catch (Exception e) {
          future.fail(e);
          return;
        }
      }
    }
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    if (!keyIsNull(k, asyncResultHandler)) {
      get(k, getEvent -> {
        if (getEvent.succeeded()) {
          delete(k, getEvent.result(), asyncResultHandler);
        } else {
          vertx.runOnContext(event -> asyncResultHandler.handle(Future.failedFuture(getEvent.cause())));
        }
      });
    }
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
    if (!keyIsNull(k, resultHandler) && !valueIsNull(v, resultHandler)) {
      get(k, getEvent -> {
        if (getEvent.succeeded()) {
          if (v.equals(getEvent.result())) {
            delete(k, v, deleteEvent -> forwardAsyncResult(resultHandler, deleteEvent, true));
          } else {
            vertx.runOnContext(event -> resultHandler.handle(Future.succeededFuture(false)));
          }
        } else {
          vertx.runOnContext(event -> resultHandler.handle(Future.failedFuture(getEvent.cause())));
        }
      });
    }
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    if (!keyIsNull(k, asyncResultHandler) && !valueIsNull(v, asyncResultHandler)) {
      get(k, getEvent -> {
        if (getEvent.succeeded()) {
          final V oldValue = getEvent.result();
          if (oldValue != null) {
            put(k, v, putEvent -> forwardAsyncResult(asyncResultHandler, putEvent, oldValue));
          } else {
            vertx.runOnContext(event -> asyncResultHandler.handle(Future.succeededFuture()));
          }
        } else {
          vertx.runOnContext(event -> asyncResultHandler.handle(Future.failedFuture(getEvent.cause())));
        }
      });
    }
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
    if (!keyIsNull(k, resultHandler) && !valueIsNull(oldValue, resultHandler) && !valueIsNull(newValue, resultHandler)) {
      get(k, getEvent -> {
        if (getEvent.succeeded()) {
          if (getEvent.result().equals(oldValue)) {
            setData(k, newValue, setEvent -> forwardAsyncResult(resultHandler, setEvent, true));
          } else {
            vertx.runOnContext(e -> resultHandler.handle(Future.succeededFuture(false)));
          }
        } else {
          vertx.runOnContext(event -> resultHandler.handle(Future.failedFuture(getEvent.cause())));
        }
      });
    }
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> resultHandler) {
    //just remove parent node
    delete(mapPath, null, deleteEvent -> forwardAsyncResult(resultHandler, deleteEvent, null));
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    try {
      curator.getChildren().inBackground((client, event) ->
          vertx.runOnContext(aVoid -> resultHandler.handle(Future.succeededFuture(event.getChildren().size()))))
          .forPath(mapPath);
    } catch (Exception e) {
      vertx.runOnContext(aVoid -> resultHandler.handle(Future.failedFuture(e)));
    }
  }

}
