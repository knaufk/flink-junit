package com.github.knaufk.flinkjunit;

import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.TestBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static com.github.knaufk.flinkjunit.FlinkJUnitRuleBuilder.AVAILABLE_PORT;
import static com.github.knaufk.flinkjunit.FlinkJUnitRuleBuilder.DEFAULT_SHUTDOWN_TIMEOUT;

public class FlinkJUnitRule extends MiniClusterResource {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJUnitRule.class);

  private Configuration configuration;
  private TestingServer localZk;

  public FlinkJUnitRule(Configuration config, Time shutdownTimeout) {
    this(getMiniClusterResourceConfigurationFrom(config, shutdownTimeout));
  }

  public FlinkJUnitRule(Configuration config) {
    this(getMiniClusterResourceConfigurationFrom(config, DEFAULT_SHUTDOWN_TIMEOUT));
  }

  /**
   * Creates a new <code>FlinkJUnitRule</code> . It will start up and tear down a local Flink
   * cluster in its <code>before</code> and <code>after</code> methods.
   *
   * @param config the configuration of the cluster
   */
  private FlinkJUnitRule(MiniClusterResourceConfiguration config) {
    super(config);
    configuration = config.getConfiguration();
  }

  /**
   * Returns the port under which the Flink UI can be reached.
   *
   * @return the port or -1 if the Flink UI is not running.
   */
  public int getFlinkUiPort() {
    return webUiEnabled() ? configuration.getInteger(WebOptions.PORT) : -1;
  }

  @Override
  public void before() throws Exception {
    if (webUiEnabled()) {
      setPortForWebUiAndUpdateConfig();
    }

    if (zookeeperHaEnabled()) {
      startLocalZookeeperAndUpdateConfig();
    }

    super.before();
  }

  @Override
  public void after() {
    super.after();
    try {
      if (zookeeperHaEnabled()) {
        stopZookeeper(localZk);
      }
    } catch (Exception e) {
      throw new FlinkJUnitException("Exception while stopping local cluster.", e);
    }
  }

  private void setPortForWebUiAndUpdateConfig() {
    if (configuration.getInteger(WebOptions.PORT) == AVAILABLE_PORT) {
      configuration.setInteger(WebOptions.PORT, availablePort());
    }
  }

  private void startLocalZookeeperAndUpdateConfig() {
    LOG.info("Zookeeper is choosen for HA. Starting local Zookeeper...");
    try {
      localZk = new TestingServer();
      int zkPort = localZk.getPort();
      localZk.start();
      configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, "localhost:" + zkPort);
      LOG.debug("Zookeeper started on port {}", zkPort);
    } catch (Exception e) {
      throw new FlinkJUnitException("Exception while starting local Zookeeper server.", e);
    }
  }

  private void stopZookeeper(final TestingServer localZk) throws IOException {
    LOG.info("Stopping local zookeeper...");
    localZk.stop();
  }

  private boolean zookeeperHaEnabled() {
    return configuration.getString(HighAvailabilityOptions.HA_MODE).equals("zookeeper");
  }

  private boolean webUiEnabled() {
    return configuration.getBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false);
  }

  /**
   * Returns a random port, which is available when the method was called.
   *
   * @return random available port
   */
  private int availablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();
      LOG.info("Setting WebUI port to random port. Port is {}.", port);
      return port;
    } catch (IOException e) {
      String msg = "Exception while finding a random port for the Flink WebUi.";
      LOG.error(msg);
      throw new FlinkJUnitException(msg, e);
    }
  }

  private static MiniClusterResourceConfiguration getMiniClusterResourceConfigurationFrom(
      Configuration flinkConfig, Time shutdownTimeout) {
    int numberOfTaskManagers = flinkConfig.getInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 0);
    int numberOfTaskSlots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 0);

    return new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(flinkConfig)
        .setNumberTaskManagers(numberOfTaskManagers)
        .setNumberSlotsPerTaskManager(numberOfTaskSlots)
        .setShutdownTimeout(shutdownTimeout)
        .setCodebaseType(TestBaseUtils.CodebaseType.LEGACY)
        .setRpcServiceSharing(RpcServiceSharing.SHARED)
        .build();
  }
}
