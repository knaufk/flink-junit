package com.github.knaufk.flinkjunit;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.awaitility.core.ThrowingRunnable;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class FlinkJunitRuleIntegrationTest {

  @Rule
  public FlinkJUnitRule flinkRule =
      new FlinkJUnitRuleBuilder()
          .withTaskmanagers(2)
          .withTaskSlots(4)
          .withWebUiEnabled(9091)
          .build();

  @Test
  public void testFlinkUiReachableUnderSpecifiedPort() throws IOException {

    await()
        .atMost(1, SECONDS)
        .untilAsserted(
            new ThrowingRunnable() {
              @Override
              public void run() throws Throwable {
                assertThat(TestUtils.getOverviewJsonFromWebUi(flinkRule.getFlinkUiPort()))
                    .contains("\"taskmanagers\":2,\"slots-total\":8");
              }
            });
  }

  @Test
  public void testStreamEnv() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(8);

    DataStream<Integer> testStream =
        env.fromElements(1, 2, 3, 4, 24, 2, 3, 23, 1, 3, 1, 13, 1, 31, 1);

    testStream.print();

    env.execute();
  }

  @Test
  public void testBatchEvn() throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(8);

    DataSource<Integer> testSet = env.fromElements(1, 2, 3, 4, 24, 2, 3, 23, 1, 3, 1, 13, 1, 31, 1);

    testSet.print();
  }
}
