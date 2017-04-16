package com.github.knaufk.flinkjunit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;

public final class FlinkJUnitRuleBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJUnitRule.class);

    public static final int DEFAULT_NUMBER_OF_TASK_SLOTS = 4;
    public static final int DEFAULT_NUMBER_OF_TASKMANAGERS = 1;

    private Optional<Integer> noOfTaskmanagers = Optional.empty();
    private Optional<Integer> noOfTaskSlots= Optional.empty();

    private Optional<Integer> webUiPort = Optional.empty();
    private boolean webUiEnabled = false;
    private boolean zookeeper = false;


    /**
     *
     * Enables Flink WebUI and binds it to a random port available.
     *
     * @return this
     */
    public FlinkJUnitRuleBuilder withWebUiEnabled() {
        this.webUiEnabled = true;
        return this;
    }


    /**
     *
     * Enables Flink WebUI and binds it to the given port.
     *
     * @param webUiPort the port to bind to
     * @return this
     */

    public FlinkJUnitRuleBuilder withWebUiEnabled(int webUiPort) {
        this.webUiEnabled = true;
        this.webUiPort = Optional.of(webUiPort);
        return this;
    }

    public FlinkJUnitRuleBuilder withTaskamangers(int noOfTaskmanagers) {
        this.noOfTaskmanagers = Optional.of(noOfTaskmanagers);
        return this;
    }

    public FlinkJUnitRuleBuilder withTaskSlots(int noOfTaskslots) {
        this.noOfTaskSlots = Optional.of(noOfTaskslots);
        return this;
    }

    /**
     *
     * Enables JobManager High-Availability for the cluster started for this test. This will spin up a local Zookeeper instance for Leader Election.
     *
     * @return
     */
    public FlinkJUnitRuleBuilder withJobManagerHA() {
        this.zookeeper = true;
        return this;
    }


    public FlinkJUnitRule build() {
        FlinkJUnitRule rule = new FlinkJUnitRule();
        rule.setNoOfTaskmanagers(noOfTaskmanagers.orElse(DEFAULT_NUMBER_OF_TASKMANAGERS));
        rule.setNoOfTaskSlots(noOfTaskSlots.orElse(DEFAULT_NUMBER_OF_TASK_SLOTS));
        rule.setWebUiEnabled(webUiEnabled);
        rule.setWebUiPort(webUiPort.orElse(availablePort()));
        rule.setZookeeper(this.zookeeper);
        return rule;
    }


    /**
     * Returns a random port, which is available when the method was called.
     *
     * @return random available port
     */
    private int availablePort() {
        try(ServerSocket socket = new ServerSocket(0)) {
            int port = socket.getLocalPort();
            LOG.info("Setting WebUI port to random port. Port is {}.", port);
            return port;
        } catch(IOException e) {
            String msg = "Exception while finding a random port for the Flink WebUi.";
            LOG.error(msg);
            throw new FlinkJUnitException(msg, e);
        }
    }

}
