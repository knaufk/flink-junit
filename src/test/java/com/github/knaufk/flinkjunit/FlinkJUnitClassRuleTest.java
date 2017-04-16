package com.github.knaufk.flinkjunit;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

public class FlinkJUnitClassRuleTest {

    @ClassRule
    public static FlinkJUnitRule flinkRule = new FlinkJUnitRuleBuilder().withTaskamangers(1)
                                                                        .withTaskSlots(4)
                                                                        .withWebUiEnabled()
                                                                        .withJobManagerHA()
                                                                        .build();

    @Test
    public void testStreamEnv() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> testStream = env.fromElements(1, 2, 3, 4, 24, 2, 3, 23, 1, 3, 1, 13, 1, 31, 1);

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