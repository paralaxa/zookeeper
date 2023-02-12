package sk.stopangin.zookeeper;

import java.nio.charset.StandardCharsets;
import java.util.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class HowItWorks extends ZkInitializer {


  private void watchRootNode() throws InterruptedException, KeeperException {
    zkConnector.getZkClient().addWatch(getRootNode(), this, AddWatchMode.PERSISTENT_RECURSIVE);
  }

  private void createChildNode() throws InterruptedException, KeeperException {
    log.info("Creating childNode");
    zkConnector.getZkClient()
        .create(getRootNode() + "/" + "childNode/newNodeEp", "childData".getBytes(
                StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL);
  }

  private void createDataNodeIfNotExist() throws InterruptedException, KeeperException {
    log.info("Creating dataNode");
    String dataNodePath = getDataNodePath();

    ZooKeeper zkClient = zkConnector.getZkClient();
    if (dataNodeDoesNotExistYet(dataNodePath)) {
      zkClient.create(dataNodePath, "someData".getBytes(StandardCharsets.UTF_8),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  private String getDataNodePath() {
    return getRootNode() + "/" + "dataNode";
  }

  private String readDataFromDataNode() throws InterruptedException, KeeperException {
    String dataNodePath = getDataNodePath();

    ZooKeeper zkClient = zkConnector.getZkClient();
    byte[] data = zkClient.getData(dataNodePath, this, null);
    return new String(data);
  }

  private void writeDataToDataNode() throws InterruptedException, KeeperException {
    String dataNodePath = getDataNodePath();

    ZooKeeper zkClient = zkConnector.getZkClient();
    zkClient.setData(dataNodePath,
        (System.currentTimeMillis() + "_newConifg").getBytes(StandardCharsets.UTF_8),2);
  }

  private boolean dataNodeDoesNotExistYet(String dataNodePath)
      throws KeeperException, InterruptedException {
    return zkConnector.getZkClient().exists(dataNodePath, false) == null;

  }

  @Override
  protected String getRootNode() {
    return "/demoRootNode";
  }

  @Override
  public void process(WatchedEvent event) {
    log.info("Event occurred:{}", event);
  }

  public static void main(String[] args) throws Exception {
    HowItWorks howItWorks = new HowItWorks();
    howItWorks.init();
    howItWorks.createRootNode();
    howItWorks.createDataNodeIfNotExist();
    log.info("Data from data node:{}", howItWorks.readDataFromDataNode());
    howItWorks.writeDataToDataNode();
    log.info("Data from data node:{}", howItWorks.readDataFromDataNode());
    howItWorks.watchRootNode();
    howItWorks.createChildNode();
    new Timer();
  }
}
