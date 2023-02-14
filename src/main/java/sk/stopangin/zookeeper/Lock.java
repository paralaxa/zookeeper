package sk.stopangin.zookeeper;

import java.util.Timer;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class Lock extends ZkInitializer {

  private static final String STOPANGINS_APP_LOCKS = "/lockApp";


  private boolean registerLock() throws InterruptedException, KeeperException {
    ZooKeeper zkClient = zkConnector.getZkClient();
    String lockPath = getLockPath();
    try {
      zkClient.create(lockPath, new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      return true;
    } catch (NodeExistsException e) {
      return false;
    }
  }

  private String getLockPath() {
    return STOPANGINS_APP_LOCKS + "/lock";
  }

  private void tryLock() throws InterruptedException, KeeperException {
    ZooKeeper zkClient = zkConnector.getZkClient();

    if (zkClient.exists(getLockPath(), false) == null) {
      if (registerLock()) {
        log.info("Locked!");
        sleepSafe();
        zkClient.close();
        log.info("Done");
      } else {
        log.info("Lock unsuccessful registering watch");
        registerWatch(zkClient);
      }
    } else {
      registerWatch(zkClient);
    }
  }

  private void registerWatch(ZooKeeper zkClient) throws KeeperException, InterruptedException {
    zkClient.exists(getLockPath(),
        this);
  }

  private void sleepSafe() {
    try {
      log.info("Processing");
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
    }
  }

  @Override
  protected String getRootNode() {
    return STOPANGINS_APP_LOCKS;
  }

  public void start() {
    try {
      init();
      createRootNode();
      tryLock();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }


  @Override
  public void process(WatchedEvent event) {
    log.info("Change detected");
    try {
      tryLock();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public static void main(String[] args) {

    Lock le = new Lock();
    le.start();
    Timer timer = new Timer();
  }


}
