package sk.stopangin.zookeeper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class CyclicBarrier extends ZkInitializer {

  private static final String STOPANGINS_APP = "/cyclicBarrierApp";

  private final CountDownLatch countDownLatch = new CountDownLatch(1);


  private void registerMyself() throws InterruptedException, KeeperException {
    String currentPid = getCurrentPid();
    ZooKeeper zkClient = zkConnector.getZkClient();
    zkClient
        .create(STOPANGINS_APP + "/barrier", currentPid.getBytes(StandardCharsets.UTF_8),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  private int getCountOfNodes() throws InterruptedException, KeeperException {
    ZooKeeper zkClient = zkConnector.getZkClient();
    List<String> children = zkClient.getChildren(STOPANGINS_APP, false);
    return children.size();
  }

  @Override
  protected String getRootNode() {
    return STOPANGINS_APP;
  }

  public void start() {
    try {
      init();
      createRootNode();
      registerMyself();
      watchForChanges();
      log.info("Waiting");
      if (getCountOfNodes() < 3) {
        countDownLatch.await();
      }
      log.info("Barrier released...");
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  private String getCurrentPid() {
    return ProcessHandle.current().pid() + "";
  }


  private void watchForChanges() throws InterruptedException, KeeperException {
    zkConnector.getZkClient().addWatch(STOPANGINS_APP, this, AddWatchMode.PERSISTENT);
  }

  @Override
  public void process(WatchedEvent event) {
    try {
      if (getCountOfNodes() == 3) {
        countDownLatch.countDown();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    log.info("Change detected");
  }

  public static void main(String[] args) {

    CyclicBarrier cb = new CyclicBarrier();
    cb.start();
    new Timer();
  }


}
