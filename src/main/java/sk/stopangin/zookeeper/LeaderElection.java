package sk.stopangin.zookeeper;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class LeaderElection extends ZkInitializer {

  private static final String STOPANGINS_APP = "/leaderApp";
  private Leader leader;


  private void registerMyself() throws InterruptedException, KeeperException {
    String currentPid = getCurrentPid();
    ZooKeeper zkClient = zkConnector.getZkClient();
    zkClient
        .create(STOPANGINS_APP + "/leader", currentPid.getBytes(StandardCharsets.UTF_8),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  private Leader getLeader() throws InterruptedException, KeeperException {
    ZooKeeper zkClient = zkConnector.getZkClient();
    List<String> children = zkClient.getChildren(STOPANGINS_APP, false);
    Collections.sort(children);

    String leaderNode = children.get(0);
    byte[] leader = zkClient.getData(STOPANGINS_APP + "/" + leaderNode, this, null);
    return new Leader(leaderNode, new String(leader));
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
      this.leader = getLeader();
      log.info("Leader on bootstrap is:{}", this.leader);
      if (amILeader()) {
        log.info("I am leader!");
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  private String getCurrentPid() {
    return ProcessHandle.current().pid() + "";
  }

  private boolean amILeader() {
    return getCurrentPid().equals(leader.getValue());
  }

  @Override
  public void process(WatchedEvent event) {
    log.info("Change detected");
    try {
      leader = getLeader();
      log.info("New leader elected:{}", leader);
      if (amILeader()) {
        log.info("Man i'm the leader!");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @Data
  @AllArgsConstructor
  private static class Leader {

    private String node;
    private String value;


  }

  public static void main(String[] args) {

    LeaderElection le = new LeaderElection();
    le.start();
    new Timer();
  }


}
