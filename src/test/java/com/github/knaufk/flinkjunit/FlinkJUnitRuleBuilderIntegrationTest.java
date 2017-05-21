package com.github.knaufk.flinkjunit;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.awaitility.core.ThrowingRunnable;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class FlinkJUnitRuleBuilderIntegrationTest {

  @ClassRule
  public static final FlinkJUnitRule flinkRule =
      new FlinkJUnitRuleBuilder()
          .withTaskmanagers(1)
          .withTaskSlots(4)
          .withWebUiEnabled()
          .withJobManagerHA()
          .build();

  @Test
  public void testFlinkUiIsReachable() throws IOException {

    await()
        .atMost(1, SECONDS)
        .untilAsserted(
            new ThrowingRunnable() {
              @Override
              public void run() throws Throwable {
                assertThat(TestUtils.getOverviewJsonFromWebUi(flinkRule.getFlinkUiPort()))
                    .contains("\"taskmanagers\":1,\"slots-total\":4");
              }
            });
  }

  @Test
  public void testStreamEnv() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Integer> testStream =
        env.fromElements(1, 2, 3, 4, 24, 2, 3, 23, 1, 3, 1, 13, 1, 31, 1);

    testStream.print();

    env.execute();
  }

  @Test
  public void testBatchEvn() throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<Integer> testSet = env.fromElements(1, 2, 3, 4, 24, 2, 3, 23, 1, 3, 1, 13, 1, 31, 1);

    testSet.print();
  }
}
