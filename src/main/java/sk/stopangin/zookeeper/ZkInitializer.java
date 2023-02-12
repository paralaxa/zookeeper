package sk.stopangin.zookeeper;

import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

@Slf4j
public abstract class ZkInitializer implements Watcher {

  protected final ZkConnector zkConnector = new ZkConnector();

  protected abstract String getRootNode();

  protected void init() {
    zkConnector.connect();
  }

  protected void createRootNode() throws InterruptedException, KeeperException {
    log.info("Creating root node:{}", getRootNode());
    if (zkConnector.getZkClient().exists(getRootNode(), false) == null) {
      log.info("Creating node:{}", getRootNode());
      try {
        zkConnector.getZkClient()
            .create(getRootNode(), "stopanginsApp management".getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
      } catch (NodeExistsException e) {
        log.debug("Node already exist", e);
      }
    } else {
      log.info("Node:{} already created", getRootNode());
    }
  }
}
