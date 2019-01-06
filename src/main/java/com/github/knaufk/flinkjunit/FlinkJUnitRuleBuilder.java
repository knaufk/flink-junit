package com.github.knaufk.flinkjunit;

import org.apache.flink.configuration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FlinkJUnitRuleBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJUnitRule.class);

  public static final int DEFAULT_NUMBER_OF_TASK_SLOTS = 4;
  public static final int DEFAULT_NUMBER_OF_TASKMANAGERS = 1;

  public static final String DEFAULT_TASK_MANAGER_MEMORY_SIZE = "80m";
  public static final long DEFAULT_AKKA_ASK_TIMEOUT = 1000;
  public static final String DEFAULT_AKKA_STARTUP_TIMEOUT = "60 s";

  public static final int AVAILABLE_PORT = 0;

  private int noOfTaskmanagers = DEFAULT_NUMBER_OF_TASKMANAGERS;
  private int noOfTaskSlots = DEFAULT_NUMBER_OF_TASK_SLOTS;

  private int webUiPort = AVAILABLE_PORT;
  private boolean webUiEnabled = false;
  private boolean zookeeperHa = false;

  /**
   * Enables Flink WebUI and binds it to a random port available.
   *
   * @return this
   */
  public FlinkJUnitRuleBuilder withWebUiEnabled() {
    this.webUiEnabled = true;
    return this;
  }

  /**
   * Enables Flink WebUI and binds it to the given port.
   *
   * @param webUiPort the port to bind to
   * @return this
   */
  public FlinkJUnitRuleBuilder withWebUiEnabled(int webUiPort) {
    this.webUiEnabled = true;
    this.webUiPort = webUiPort;
    return this;
  }

  public FlinkJUnitRuleBuilder withTaskmanagers(int noOfTaskmanagers) {
    this.noOfTaskmanagers = noOfTaskmanagers;
    return this;
  }

  public FlinkJUnitRuleBuilder withTaskSlots(int noOfTaskSlots) {
    this.noOfTaskSlots = noOfTaskSlots;
    return this;
  }

  /**
   * Enables JobManager high availability for the cluster started for this test. This will spin up a
   * local Zookeeper instance for leader election.
   *
   * @return this
   */
  public FlinkJUnitRuleBuilder withJobManagerHA() {
    this.zookeeperHa = true;
    return this;
  }

  public FlinkJUnitRule build() {
    return new FlinkJUnitRule(buildConfiguration());
  }

  private Configuration buildConfiguration() {

    Configuration flinkConfig = new Configuration();

    flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, noOfTaskmanagers);
    flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, noOfTaskSlots);
    flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, webUiEnabled);
    flinkConfig.setInteger(WebOptions.PORT, webUiPort);

    flinkConfig.setString(
        TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, DEFAULT_TASK_MANAGER_MEMORY_SIZE);
    flinkConfig.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
    flinkConfig.setString(AkkaOptions.ASK_TIMEOUT, DEFAULT_AKKA_ASK_TIMEOUT + "s");
    flinkConfig.setString(AkkaOptions.STARTUP_TIMEOUT, DEFAULT_AKKA_STARTUP_TIMEOUT);

    if (zookeeperHa) {
      flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 3);
      flinkConfig.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
      flinkConfig.setString(HighAvailabilityOptions.HA_STORAGE_PATH, "/tmp/flink");
    }

    return flinkConfig;
  }
}
