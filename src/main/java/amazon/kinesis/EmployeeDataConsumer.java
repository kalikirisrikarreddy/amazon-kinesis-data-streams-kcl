package amazon.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

public class EmployeeDataConsumer {

  private String streamName;
  private Region region;
  private KinesisAsyncClient kinesisClient;
  private ObjectMapper objectMapper = new ObjectMapper();
  private ObjectReader employeeReader = objectMapper.readerFor(Employee.class);
  private AtomicInteger recordCount = new AtomicInteger(0);

  public static void main(String... args) {
    String streamName = "employee-data-stream";
    String region = "us-west-2";
    new EmployeeDataConsumer(streamName, region).run();
  }

  private EmployeeDataConsumer(String streamName, String region) {
    this.streamName = streamName;
    this.region = Region.of(ObjectUtils.firstNonNull(region, "us-west-2"));
    this.kinesisClient =
        KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(this.region));
  }

  private void run() {
    System.out.println("Running consumer...");
    DynamoDbAsyncClient dynamoClient =
        DynamoDbAsyncClient.builder().region(region).build();
    CloudWatchAsyncClient cloudWatchClient =
        CloudWatchAsyncClient.builder().region(region).build();
    ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, streamName,
        kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(),
        new CustomShardRecordProcessorFactory());
    configsBuilder.streamTracker(new SingleStreamTracker(streamName,
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)));
    PollingConfig pollingConfig = new PollingConfig(streamName, kinesisClient);
    pollingConfig.idleTimeBetweenReadsInMillis(10000L);
    Scheduler scheduler = new Scheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        configsBuilder.retrievalConfig().retrievalSpecificConfig(pollingConfig)
    );

    scheduler.run();
  }

  private class CustomShardRecordProcessorFactory implements ShardRecordProcessorFactory {

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
      return new CustomShardRecordProcessor();
    }

  }

  private class CustomShardRecordProcessor implements ShardRecordProcessor {

    private String shardId;

    @Override
    public void initialize(InitializationInput initializationInput) {
      shardId = initializationInput.shardId();
      System.out.println("Initializing @ Sequence: "+ initializationInput.extendedSequenceNumber()
          + ", Shard: " + initializationInput.shardId());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
      try {
        System.out.println("Processing record(s): " +  processRecordsInput.records().size());
        recordCount.getAndAdd(processRecordsInput.records().size());
        processRecordsInput.records().forEach(r ->
        {
          try {
            System.out.println("Processing record, aggregated: " + r.aggregated() +
                ", pk: " + r.partitionKey() + ", -- Seq: " + r.sequenceNumber() +
                ", -- Sub Seq: " + r.subSequenceNumber() + ", -- Data: "
                + employeeReader.readValue(StandardCharsets.UTF_8.decode(r.data()).toString()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      } catch (Throwable t) {
        t.printStackTrace();
        Runtime.getRuntime().halt(1);
      }
      System.out.println("recordCount = " + recordCount.get());
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
      System.out.println("Lost lease, so terminating.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
      System.out.println("Reached shard end checkpointing.");
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
      System.out.println("Scheduler is shutting down, checkpointing.");
    }
  }

}
