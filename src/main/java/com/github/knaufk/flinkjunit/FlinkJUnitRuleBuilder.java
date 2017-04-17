package com.github.knaufk.flinkjunit;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FlinkJUnitRuleBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJUnitRule.class);

  public static final int DEFAULT_NUMBER_OF_TASK_SLOTS = 4;
  public static final int DEFAULT_NUMBER_OF_TASKMANAGERS = 1;

  public static final long DEFAULT_TASK_MANAGER_MEMORY_SIZE = 80;
  public static final long DEFAULT_AKKA_ASK_TIMEOUT = 1000;
  public static final String DEFAULT_AKKA_STARTUP_TIMEOUT = "60 s";

  private int noOfTaskmanagers = DEFAULT_NUMBER_OF_TASKMANAGERS;
  private int noOfTaskSlots = DEFAULT_NUMBER_OF_TASK_SLOTS;

  private int webUiPort = 0;
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
   * @return
   */
  public FlinkJUnitRuleBuilder withJobManagerHA() {
    this.zookeeperHa = true;
    return this;
  }

  public FlinkJUnitRule build() {
    return new FlinkJUnitRule(buildConfiguration());
  }

  private Configuration buildConfiguration() {

    Configuration config = new Configuration();

    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, noOfTaskmanagers);
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, noOfTaskSlots);
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, webUiEnabled);
    config.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, webUiPort);

    config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, DEFAULT_TASK_MANAGER_MEMORY_SIZE);
    config.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, true);
    config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, DEFAULT_AKKA_ASK_TIMEOUT + "s");
    config.setString(ConfigConstants.AKKA_STARTUP_TIMEOUT, DEFAULT_AKKA_STARTUP_TIMEOUT);

    if (zookeeperHa) {
      config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 3);
      config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
      config.setString(ConfigConstants.HA_ZOOKEEPER_STORAGE_PATH, "/tmp/flink");
    }

    return config;
  }
}
