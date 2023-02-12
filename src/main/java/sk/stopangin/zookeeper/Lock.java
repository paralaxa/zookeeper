package sk.stopangin.zookeeper;

import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class Lock extends ZkInitializer {

  private static final String STOPANGINS_APP_LOCKS = "/stopanginsApp/locks";

  private String thisLockNode;


  private void registerLock() throws InterruptedException, KeeperException {
    ZooKeeper zkClient = zkConnector.getZkClient();
    String lockPath = STOPANGINS_APP_LOCKS + "/lock";
    thisLockNode = zkClient
        .create(lockPath, new byte[1],
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  private void tryLock() throws InterruptedException, KeeperException {
    ZooKeeper zkClient = zkConnector.getZkClient();
    List<String> children = zkClient.getChildren(STOPANGINS_APP_LOCKS, false); //heard effect
    Collections.sort(children);
    String actuallyHeldLock = children.get(0);
    if (thisLockNode.contains(actuallyHeldLock)) {
      log.info("Locked!");
      sleepSafe();
      zkClient.delete(STOPANGINS_APP_LOCKS + "/" + actuallyHeldLock, 0);
      log.info("Done");
    } else {
      zkClient.exists(STOPANGINS_APP_LOCKS + "/" + actuallyHeldLock,
          this); //aby som nemal heard effect, registrujem len na tento node
    }
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
      registerLock();
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
