package sk.stopangin.zookeeper;

import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZkConnector implements Watcher, AutoCloseable {

  @Getter
  private ZooKeeper zkClient;
  private final CountDownLatch latch = new CountDownLatch(1);

  public void connect() {
    try {
      zkClient = new ZooKeeper("localhost:2181", 3000, this);
      latch.await();
    } catch (Exception e) {
      throw new ZkConnectionException(e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected) {
      latch.countDown();

    }
  }

  @Override
  public void close() throws Exception {
    try {
      zkClient.close();
    } catch (InterruptedException e) {
    }
  }
}
