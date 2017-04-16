package com.github.knaufk.flinkjunit;

import akka.actor.ActorRef;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FlinkJUnitRule extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJUnitRule.class);

    private static final int DEFAULT_PARALLELISM = 4;

    private static final long   TASK_MANAGER_MEMORY_SIZE     = 80;
    private static final long   DEFAULT_AKKA_ASK_TIMEOUT     = 1000;
    private static final String DEFAULT_AKKA_STARTUP_TIMEOUT = "60 s";

    private int     noOfTaskmanagers;
    private int     noOfTaskSlots;
    private boolean webUiEnabled;
    private int     webUiPort;

    private LocalFlinkMiniCluster miniCluster;
    private boolean               zookeeper;

    FlinkJUnitRule() {
    }

    @Override
    protected void before() throws Throwable {
        miniCluster = startCluster(createConfiguration(), false);

        TestStreamEnvironment.setAsContext(miniCluster, DEFAULT_PARALLELISM);
        TestEnvironment testEnvironment = new TestEnvironment(miniCluster, DEFAULT_PARALLELISM);
        testEnvironment.setAsContext();
    }

    @Override
    protected void after() {
        try {
            stopCluster(miniCluster, new FiniteDuration(1, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new FlinkJUnitException("Exception while stopping local cluster.", e);
        } finally {
            TestStreamEnvironment.unsetAsContext();
        }
    }

    private Configuration createConfiguration() throws Exception {

        Configuration config = new Configuration();

        //Configuration by user
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, noOfTaskmanagers);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, noOfTaskSlots);
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, webUiEnabled);
        config.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, webUiPort);

        //Defaults
        config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, TASK_MANAGER_MEMORY_SIZE);
        config.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, true);
        config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, DEFAULT_AKKA_ASK_TIMEOUT + "s");
        config.setString(ConfigConstants.AKKA_STARTUP_TIMEOUT, DEFAULT_AKKA_STARTUP_TIMEOUT);


        if (zookeeper) {
            TestingServer zookeeperServer = new TestingServer();
            config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 3);
            config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
            config.setString(ConfigConstants.HA_ZOOKEEPER_STORAGE_PATH, "/tmp/flink");
            config.setString(ConfigConstants.HA_ZOOKEEPER_QUORUM_KEY, "localhost:"+zookeeperServer.getPort());

        }

        return config;
    }


    private LocalFlinkMiniCluster startCluster(
            Configuration config,
            boolean singleActorSystem) throws Exception {

        LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(config, singleActorSystem);

        cluster.start();

        return cluster;
    }

    private void stopCluster(LocalFlinkMiniCluster executor, FiniteDuration timeout) throws Exception {
        if (executor != null) {
            int numUnreleasedBCVars = 0;
            int numActiveConnections = 0;

            if (executor.running()) {
                List<ActorRef> tms = executor.getTaskManagersAsJava();
                List<Future<Object>> bcVariableManagerResponseFutures = new ArrayList<>();
                List<Future<Object>> numActiveConnectionsResponseFutures = new ArrayList<>();

                for (ActorRef tm : tms) {
                    bcVariableManagerResponseFutures.add(Patterns.ask(
                            tm,
                            TaskManagerMessages.getRequestBroadcastVariablesWithReferences(),
                            new Timeout(timeout)));

                    numActiveConnectionsResponseFutures.add(Patterns.ask(
                            tm,
                            TaskManagerMessages.getRequestNumActiveConnections(),
                            new Timeout(timeout)));
                }

                Future<Iterable<Object>> bcVariableManagerFutureResponses = Futures.sequence(
                        bcVariableManagerResponseFutures, defaultExecutionContext());

                Iterable<Object> responses = Await.result(bcVariableManagerFutureResponses, timeout);

                for (Object response : responses) {
                    numUnreleasedBCVars += ((TaskManagerMessages.ResponseBroadcastVariablesWithReferences) response).number();
                }

                Future<Iterable<Object>> numActiveConnectionsFutureResponses = Futures.sequence(
                        numActiveConnectionsResponseFutures, defaultExecutionContext());

                responses = Await.result(numActiveConnectionsFutureResponses, timeout);

                for (Object response : responses) {
                    numActiveConnections += ((TaskManagerMessages.ResponseNumActiveConnections) response).number();
                }
            }

            executor.stop();
            FileSystem.closeAll();

            Assert.assertEquals("Not all broadcast variables were released.", 0, numUnreleasedBCVars);
            Assert.assertEquals("Not all TCP connections were released.", 0, numActiveConnections);
        }
    }

    private ExecutionContext defaultExecutionContext() {
        return ExecutionContext$.MODULE$.global();
    }

    public void setNoOfTaskmanagers(final int noOfTaskmanagers) {
        this.noOfTaskmanagers = noOfTaskmanagers;
    }

    public void setNoOfTaskSlots(final int noOfTaskSlots) {
        this.noOfTaskSlots = noOfTaskSlots;
    }

    public void setWebUiEnabled(final boolean webUiEnabled) {
        this.webUiEnabled = webUiEnabled;
    }

    public void setWebUiPort(final int webUiPort) {
        this.webUiPort = webUiPort;
    }

    public void setZookeeper(final boolean zookeeper) {
        this.zookeeper = zookeeper;
    }
}
