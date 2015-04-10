package io.vertx.spi.cluster.impl.zookeeper;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.zookeeper.CreateMode;

import java.io.*;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Created by Stream.Liu
 */
abstract class ZKMap<K, V> {

  protected final CuratorFramework curator;
  protected final Vertx vertx;
  protected final String mapPath;
  protected final String mapName;

  protected static final String ZK_PATH_ASYNC_MAP = "asyncMap";
  protected static final String ZK_PATH_ASYNC_MULTI_MAP = "asyncMultiMap";
  protected static final String ZK_PATH_SYNC_MAP = "syncMap";

  protected ZKMap(CuratorFramework curator, Vertx vertx, String mapType, String mapName) {
    this.curator = curator;
    this.vertx = vertx;
    this.mapName = mapName;
    this.mapPath = "/" + mapType + "/" + mapName;
  }

  protected String keyPath(K k) {
    Objects.requireNonNull(k, "key should not be null.");
    return mapPath + "/" + k.toString();
  }

  protected String valuePath(K k, Object v) {
    Objects.requireNonNull(v, "value should not be null.");
    return keyPath(k) + "/" + v.hashCode();
  }

  protected <T> boolean keyIsNull(Object key, Handler<AsyncResult<T>> handler) {
    boolean result = key == null;
    if (result) handler.handle(Future.failedFuture("key can not be null."));
    return result;
  }

  protected <T> boolean valueIsNull(Object value, Handler<AsyncResult<T>> handler) {
    boolean result = value == null;
    if (result) handler.handle(Future.failedFuture("value can not be null."));
    return result;
  }

  protected byte[] asByte(Object object) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(byteOut);
    if (object instanceof ClusterSerializable) {
      ClusterSerializable clusterSerializable = (ClusterSerializable) object;
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(object.getClass().getName());
      byte[] bytes = clusterSerializable.writeToBuffer().getBytes();
      dataOutput.writeInt(bytes.length);
      dataOutput.write(bytes);
    } else {
      dataOutput.writeBoolean(false);
      ByteArrayOutputStream javaByteOut = new ByteArrayOutputStream();
      ObjectOutput objectOutput = new ObjectOutputStream(javaByteOut);
      objectOutput.writeObject(object);
      dataOutput.write(javaByteOut.toByteArray());
    }
    return byteOut.toByteArray();
  }

  protected <T> T asObject(byte[] bytes) throws Exception {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    DataInputStream in = new DataInputStream(byteIn);
    boolean isClusterSerializable = in.readBoolean();
    if (isClusterSerializable) {
      String className = in.readUTF();
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      int length = in.readInt();
      byte[] body = new byte[length];
      in.readFully(body);
      try {
        ClusterSerializable clusterSerializable = (ClusterSerializable) clazz.newInstance();
        clusterSerializable.readFromBuffer(Buffer.buffer(body));
        return (T) clusterSerializable;
      } catch (Exception e) {
        throw new IllegalStateException("Failed to load class " + e.getMessage(), e);
      }
    } else {
      byte[] body = new byte[in.available()];
      in.readFully(body);
      ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(body));
      return (T) objectIn.readObject();
    }
  }

  protected <T, E> void forwardAsyncResult(Handler<AsyncResult<T>> completeHandler, AsyncResult<E> asyncResult) {
    if (asyncResult.succeeded()) {
      E result = asyncResult.result();
      if (result == null || result instanceof Void) {
        vertx.runOnContext(event -> completeHandler.handle(Future.succeededFuture()));
      } else {
        vertx.runOnContext(event -> completeHandler.handle(Future.succeededFuture((T) result)));
      }
    } else {
      vertx.runOnContext(aVoid -> completeHandler.handle(Future.failedFuture(asyncResult.cause())));
    }
  }

  protected <T, E> void forwardAsyncResult(Handler<AsyncResult<T>> completeHandler, AsyncResult<E> asyncResult, T result) {
    if (asyncResult.succeeded()) {
      vertx.runOnContext(event -> completeHandler.handle(Future.succeededFuture(result)));
    } else {
      vertx.runOnContext(aVoid -> completeHandler.handle(Future.failedFuture(asyncResult.cause())));
    }
  }

  protected void checkExists(K k, AsyncResultHandler<Boolean> handler) {
    checkExists(keyPath(k), handler);
  }

  protected void checkExists(String path, AsyncResultHandler<Boolean> handler) {
    try {
      curator.checkExists().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.EXISTS) {
          if (event.getStat() == null) {
            vertx.runOnContext(aVoid -> handler.handle(Future.succeededFuture(false)));
          } else {
            vertx.runOnContext(aVoid -> handler.handle(Future.succeededFuture(true)));
          }
        }
      }).forPath(path);
    } catch (Exception ex) {
      vertx.runOnContext(aVoid -> handler.handle(Future.failedFuture(ex)));
    }
  }

  protected void create(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    create(keyPath(k), v, completionHandler);
  }

  protected void create(String path, V v, Handler<AsyncResult<Void>> completionHandler) {
    try {
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground((cl, el) -> {
        if (el.getType() == CuratorEventType.CREATE) {
          vertx.runOnContext(event -> completionHandler.handle(Future.succeededFuture()));
        }
      }).forPath(path, asByte(v));
    } catch (Exception ex) {
      vertx.runOnContext(event -> completionHandler.handle(Future.failedFuture(ex)));
    }
  }

  protected void setData(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    setData(keyPath(k), v, completionHandler);
  }

  protected void setData(String path, V v, Handler<AsyncResult<Void>> completionHandler) {
    try {
      curator.setData().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.SET_DATA) {
          vertx.runOnContext(e -> completionHandler.handle(Future.succeededFuture()));
        }
      }).forPath(path, asByte(v));
    } catch (Exception ex) {
      vertx.runOnContext(event -> completionHandler.handle(Future.failedFuture(ex)));
    }
  }

  protected void delete(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    delete(keyPath(k), v, asyncResultHandler);
  }

  protected void delete(String path, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    try {
      curator.delete().deletingChildrenIfNeeded().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.DELETE) {
          curator.getChildren().inBackground((childClient, childEvent) -> {
            if (childEvent.getChildren().size() == 0) {
              //clean parent node if doesn't have child node.
              String[] paths = path.split("/");
              String parentNodePath = Stream.of(paths).limit(paths.length - 1).reduce((previous, current) -> previous + "/" + current).get();
              curator.delete().inBackground((client1, event1) ->
                  vertx.runOnContext(ea -> asyncResultHandler.handle(Future.succeededFuture(v)))).forPath(parentNodePath);
            } else {
              vertx.runOnContext(ea -> asyncResultHandler.handle(Future.succeededFuture(v)));
            }
          }).forPath(path);
        }
      }).forPath(path);
    } catch (Exception ex) {
      vertx.runOnContext(aVoid -> asyncResultHandler.handle(Future.failedFuture(ex)));
    }
  }
}
