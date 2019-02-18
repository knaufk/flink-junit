[![Build Status](https://travis-ci.org/knaufk/flink-junit.svg?branch=master)](https://travis-ci.org/knaufk/flink-junit) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.knaufk/flink-junit_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.knaufk/flink-junit_2.11)


## JUnit Rule for Apache Flink

This is a small, easy-to-use, but flexible JUnit Rule, which spins up and tears down an Apache Flink cluster for integration tests. 

It builds upon Flink's `TestEnvironment` and `TestStreamEnvironment` and can be used for the `DataSet API` as well as `DataStream API`. 

### Verify 

`./gradlew check`

### Compatability

| Version |  Flink Version | Java Version |
| --------------- | ------------- | ------------ |
| 0.5    | 1.6.x  | 1.8 |
| 0.4    | 1.5.x | 1.7 |
| 0.3    | 1.4.x  | 1.7 |
| 0.2    | 1.3.x  | 1.7 |
| 0.1    | 1.2.x  | 1.7 |


### Dependencies

Release versions are available on Maven Central. Snapshot versions are available in Sonatype OSS Snapshot Repository. 


##### Gradle Example

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://oss.sonatype.org/content/repositories/snapshots"
    }
}

dependencies {
    testCompile 'com.github.knaufk:flink-junit_2.11:0.5'
    testCompile 'junit:junit:4.11'
}
```


### Usage

There are two ways to use this rule. 

##### Option 1: FlinkJunitRuleBuilder

The preferred way to use the `FlinkJunitRule` is to use the `FlinkJUnitRuleBuilder`. This way you can easily configure a local Flink cluster with a few lines of code. 
 ```java
  @ClassRule
  public static FlinkJUnitRule flinkRule = new FlinkJUnitRuleBuilder()
                                                .withTaskmanagers(1)    
                                                .withTaskSlots(4)
                                                .withWebUiEnabled() // Will use random free port
                                                .withJobManagerHA() // Will spin up local Zookeeper broker (random free port)
                                                .build();
 
   @Test
   public void testingAStreamingJob() throws Exception {
 
     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 
     DataStream<Integer> testStream = env.fromElements(1, 2, 3, 4);
 
     testStream.print();
 
     env.execute();
   }
 
   @Test
   public void testingABatchJob() throws Exception {
 
     ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 
     DataSource<Integer> testSet = env.fromElements(1, 2, 3, 4);
 
     testSet.print();
   }
 
 ```
 
There are reasonable defaults for all options. To get started, you can just use `new FlinkJunitBuilder().build()`.
 
##### Option 2: Full  Flexibility
 
For full configurability you can also pass the full Flink cluster configuration object to the constructor of the `FlinkJunitRule` directly. This option exists to give the application developer the full flexbility to adjust the cluster to the specific needs of the test.

 ```java
   private static Configuration config = new Configuration();

   static {
     config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
     ...
   }

   @ClassRule
   public static FlinkJUnitRule flinkRule = new FlinkJUnitRule(config);
 ```
  
##### @Rule vs @ClassRule

When using `@ClassRule` one Flink cluster will be used for all test methods. With `@Rule` a new Flink cluster will be started and torn down for each test. In most cases `@ClassRule` should work fine and saves time during test execution as opposed to `@Rule`
