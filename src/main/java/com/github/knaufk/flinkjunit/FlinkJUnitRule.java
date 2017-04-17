package com.github.knaufk.flinkjunit;

import akka.actor.ActorRef;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.curator.test.TestingServer;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ConfigConstants.HA_ZOOKEEPER_QUORUM_KEY;

public class FlinkJUnitRule extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJUnitRule.class);

  private static final int DEFAULT_PARALLELISM = 4;

  /**
   * Creates a new <code>FlinkJUnitRule</code> . It will start up and tear down a local Flink
   * cluster in its <code>before</code> and <code>after</code> methods.
   *
   * @param configuration the configuration of the cluster
   */
  public FlinkJUnitRule(Configuration configuration) {
    this.configuration = configuration;
  }

  private Configuration configuration;
  private LocalFlinkMiniCluster miniCluster;

  private TestingServer localZk;

  @Override
  protected void before() throws Throwable {
    if (zookeeperHAEnabled()) {
      startLocalZookeeperAndUpdateConfig();
    }
    miniCluster = startCluster();
    setEnvContextToMiniCluster(miniCluster);
  }

  @Override
  protected void after() {
    try {
      stopCluster(miniCluster, new FiniteDuration(1, TimeUnit.SECONDS));
      if (zookeeperHAEnabled()) {
        stopZookeeper(localZk);
      }
    } catch (Exception e) {
      throw new FlinkJUnitException("Exception while stopping local cluster.", e);
    } finally {
      TestStreamEnvironment.unsetAsContext();
    }
  }

  private LocalFlinkMiniCluster startCluster() {
    LocalFlinkMiniCluster miniCluster = new LocalFlinkMiniCluster(configuration, false);
    miniCluster.start();
    return miniCluster;
  }

  private void setEnvContextToMiniCluster(final LocalFlinkMiniCluster miniCluster) {
    TestStreamEnvironment.setAsContext(miniCluster, DEFAULT_PARALLELISM);
    TestEnvironment testEnvironment = new TestEnvironment(miniCluster, DEFAULT_PARALLELISM);
    testEnvironment.setAsContext();
  }

  private void startLocalZookeeperAndUpdateConfig() throws Exception {
    LOG.info("Zookeeper is choosen for HA. Starting local Zookeeper...");
    localZk = new TestingServer();
    int zkPort = localZk.getPort();
    configuration.setString(HA_ZOOKEEPER_QUORUM_KEY, "localhost:" + zkPort);
    localZk.start();
    LOG.debug("Zookeeper started on port {}", zkPort);
  }

  private void stopZookeeper(final TestingServer localZk) throws IOException {
    LOG.info("Stopping local zookeeper...");
    localZk.stop();
  }

  private boolean zookeeperHAEnabled() {
    return configuration.getString(HighAvailabilityOptions.HA_MODE).equals("zookeeper");
  }

  private void stopCluster(LocalFlinkMiniCluster executor, FiniteDuration timeout)
      throws Exception {
    if (executor != null) {
      int numUnreleasedBCVars = 0;
      int numActiveConnections = 0;

      if (executor.running()) {
        List<ActorRef> tms = executor.getTaskManagersAsJava();
        List<Future<Object>> bcVariableManagerResponseFutures = new ArrayList<>();
        List<Future<Object>> numActiveConnectionsResponseFutures = new ArrayList<>();

        for (ActorRef tm : tms) {
          bcVariableManagerResponseFutures.add(
              Patterns.ask(
                  tm,
                  TaskManagerMessages.getRequestBroadcastVariablesWithReferences(),
                  new Timeout(timeout)));

          numActiveConnectionsResponseFutures.add(
              Patterns.ask(
                  tm, TaskManagerMessages.getRequestNumActiveConnections(), new Timeout(timeout)));
        }

        Future<Iterable<Object>> bcVariableManagerFutureResponses =
            Futures.sequence(bcVariableManagerResponseFutures, defaultExecutionContext());

        Iterable<Object> responses = Await.result(bcVariableManagerFutureResponses, timeout);

        for (Object response : responses) {
          numUnreleasedBCVars +=
              ((TaskManagerMessages.ResponseBroadcastVariablesWithReferences) response).number();
        }

        Future<Iterable<Object>> numActiveConnectionsFutureResponses =
            Futures.sequence(numActiveConnectionsResponseFutures, defaultExecutionContext());

        responses = Await.result(numActiveConnectionsFutureResponses, timeout);

        for (Object response : responses) {
          numActiveConnections +=
              ((TaskManagerMessages.ResponseNumActiveConnections) response).number();
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
}
