package org.bf2.performance;

import io.fabric8.kubernetes.api.model.Quantity;
import io.openmessaging.benchmark.TestResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.performance.framework.KubeClusterResource;
import org.bf2.performance.framework.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Testcase 1: Producer throughput with a single small Kafka cluster (K2)
 */
@Tag(TestTags.PERF)
@ExtendWith(KafkaConnectionDetailsParameterResolver.class)
public class KafkaInstanceScalingSmallTestDecoupled extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaInstanceScalingSmallTestDecoupled.class);
    private static final Quantity WORKER_SIZE = Quantity.parse("2Gi");
    static OMB omb;

    List<String> workers;
    List<KafkaConnectionDetails> kafkas;
    static KubeClusterResource kafkaCluster;
    static ManagedKafkaProvisioner kafkaProvisioner;

    @BeforeAll
    void beforeAll(List<KafkaConnectionDetails> kafkas) throws Exception {
        this.kafkas = kafkas;
        omb = new OMB(KubeClusterResource.connectToKubeCluster(PerformanceEnvironment.OMB_KUBECONFIG));
        omb.install(null);
    }

    @AfterAll
    void afterAll() throws Exception {
        omb.uninstall();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        omb.setWorkerContainerMemory(WORKER_SIZE);
    }

    @AfterEach
    void afterEach() throws Exception {
        omb.deleteWorkers();
    }

    @Test
    void testClusterMaxKafkaValueProdBinPacking(TestInfo info) throws Exception {
        
        assumeTrue(kafkas.size() > 0, "No Kafka instances provided");
        workers = omb.deployWorkers(kafkas.size() * PerformanceEnvironment.WORKERS_PER_INSTANCE);

        Map<KafkaConnectionDetails, List<String>> workerMapping = new HashMap<>();

        Iterator<String> workerIt = workers.iterator();
        for (KafkaConnectionDetails kafkaConnectionDetails : kafkas) {
            List<String> ws = new ArrayList<>();
            for (int w = 0; w < PerformanceEnvironment.WORKERS_PER_INSTANCE; w++) {
                ws.add(workerIt.next());
            }
            workerMapping.put(kafkaConnectionDetails, ws);
        }
        
        // int messageSize = 1024;
        // int targetRate = 30_000;
        // ManagedKafkaCapacity capacity = kafkaProvisioner.defaultCapacity((long) targetRate * messageSize * 2);
        
        // ManagedKafkaDeployment kafkaDeployment = kafkaProvisioner.deployCluster("cluster1", capacity, AdopterProfile.VALUE_PROD);
        // workers = omb.deployWorkers(PerformanceEnvironment.WORKERS_PER_INSTANCE);
        // Map<ManagedKafkaDeployment, List<String>> workerMapping = new HashMap<>();

        // Iterator<String> workerIt = workers.iterator();
        // Map<ManagedKafkaDeployment, String> instanceBootstrap = new HashMap<>();
        // List<String> ws = new ArrayList<>();
        // for (int w = 0; w < PerformanceEnvironment.WORKERS_PER_INSTANCE; w++) {
        //     ws.add(workerIt.next());
        // }
        // workerMapping.put(kafkaDeployment, ws);
        // instanceBootstrap.put(kafkaDeployment, kafkaDeployment.waitUntilReady());

        ExecutorService executorService = Executors.newFixedThreadPool(kafkas.size());
        AtomicInteger timeout = new AtomicInteger();
        List<TestResult> testResults = new ArrayList<>();
        try {
            List<Future<OMBWorkloadResult>> results = new ArrayList<>();
            for (KafkaConnectionDetails kafkaConnectionDetails : kafkas) {
                File ombDir = new File(instanceDir, kafkaConnectionDetails.getBootstrapURL());
                Files.createDirectories(ombDir.toPath());

                OMBDriver driver = new OMBDriver()
                    .setReplicationFactor(3)
                    .setTopicConfig("min.insync.replicas=2\n")
                    .setCommonConfig(kafkaConnectionDetails.toString())
                    .setProducerConfig("acks=all\n")
                    .setConsumerConfig("auto.offset.reset=earliest\nenable.auto.commit=false\n");

                OMBWorkload workload = new OMBWorkload()
                    .setName(String.format("Kafka Cluster: %s", kafkaConnectionDetails.getBootstrapURL()))
                    .setTopics(PerformanceEnvironment.TOPICS_PER_KAFKA)
                    .setPartitionsPerTopic(PerformanceEnvironment.PARTITIONS_PER_TOPIC)
                    .setMessageSize(Quantity.getAmountInBytes(PerformanceEnvironment.PAYLOAD_FILE_SIZE).intValueExact())
                    .setPayloadFile(TestUtils.payloadFileOfSize(PerformanceEnvironment.PAYLOAD_FILE_SIZE))
                    .setSubscriptionsPerTopic(PerformanceEnvironment.SUBSCRIPTIONS_PER_TOPIC)
                    .setConsumerPerSubscription(PerformanceEnvironment.CONSUMER_PER_SUBSCRIPTION)
                    .setProducersPerTopic(PerformanceEnvironment.PRODUCERS_PER_TOPIC)
                    .setProducerRate(PerformanceEnvironment.TARGET_RATE)
                    .setConsumerBacklogSizeGB(0);
                timeout.set(Math.max(workload.getTestDurationMinutes() + workload.getWarmupDurationMinutes(), timeout.get()));

                results.add(executorService.submit(() -> {
                    OMBWorkloadResult result = omb.runWorkload(ombDir, driver, workerMapping.get(kafkaConnectionDetails), workload);
                    LOGGER.info("Result stored in {}", result.getResultFile().getAbsolutePath());
                    return result;
                }));
            }
            for (Future<OMBWorkloadResult> result : results) {
                testResults.add(result.get(timeout.get() * 2L, TimeUnit.MINUTES).getTestResult());
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }

        // ExecutorService executorService = Executors.newFixedThreadPool(1);
        // AtomicInteger timeout = new AtomicInteger();
        // List<TestResult> testResults = new ArrayList<>();
        // try {
        //     List<Future<OMBWorkloadResult>> results = new ArrayList<>();
        //     for (Map.Entry<ManagedKafkaDeployment, String> entry : instanceBootstrap.entrySet()) {
        //         File ombDir = new File(instanceDir, entry.getKey().getManagedKafka().getMetadata().getName());
        //         Files.createDirectories(ombDir.toPath());

        //         OMBDriver driver = new OMBDriver()
        //                 .setReplicationFactor(3)
        //                 .setTopicConfig("min.insync.replicas=2\n")
        //                 .setCommonConfigWithBootstrapUrl(entry.getValue())
        //                 .setProducerConfig("acks=all\n")
        //                 .setConsumerConfig("auto.offset.reset=earliest\nenable.auto.commit=false\n");

        //         OMBWorkload workload = new OMBWorkload()
        //                 .setName(String.format("Kafka Cluster: %s", entry.getKey().getManagedKafka().getMetadata().getName()))
        //                 .setTopics(PerformanceEnvironment.TOPICS_PER_KAFKA)
        //                 .setPartitionsPerTopic(PerformanceEnvironment.PARTITIONS_PER_TOPIC)
        //                 .setMessageSize(Quantity.getAmountInBytes(PerformanceEnvironment.PAYLOAD_FILE_SIZE).intValueExact())
        //                 .setPayloadFile(TestUtils.payloadFileOfSize(PerformanceEnvironment.PAYLOAD_FILE_SIZE))
        //                 .setSubscriptionsPerTopic(PerformanceEnvironment.SUBSCRIPTIONS_PER_TOPIC)
        //                 .setConsumerPerSubscription(PerformanceEnvironment.CONSUMER_PER_SUBSCRIPTION)
        //                 .setProducersPerTopic(PerformanceEnvironment.PRODUCERS_PER_TOPIC)
        //                 .setProducerRate(PerformanceEnvironment.TARGET_RATE)
        //                 .setConsumerBacklogSizeGB(0);
        //         timeout.set(Math.max(workload.getTestDurationMinutes() + workload.getWarmupDurationMinutes(), timeout.get()));

        //         results.add(executorService.submit(() -> {
        //             OMBWorkloadResult result = omb.runWorkload(ombDir, driver, workerMapping.get(entry.getKey()), workload);
        //             LOGGER.info("Result stored in {}", result.getResultFile().getAbsolutePath());
        //             return result;
        //         }));
        //     }
        //     for (Future<OMBWorkloadResult> result : results) {
        //         testResults.add(result.get(timeout.get() * 2L, TimeUnit.MINUTES).getTestResult());
        //     }
        // } finally {
        //     executorService.shutdown();
        //     executorService.awaitTermination(1, TimeUnit.MINUTES);
        // }

        double threshold = 0.75 * PerformanceEnvironment.TARGET_RATE;
        List<Executable> assertions = new ArrayList<>();
        for (TestResult workloadResult : testResults) {
            List<Double> lowProduceRates = workloadResult.publishRate.stream().filter(rate -> rate < threshold).collect(Collectors.toList());
            List<Double> lowConsumeRates = workloadResult.consumeRate.stream().filter(rate -> rate < threshold).collect(Collectors.toList());
            assertions.add(() -> assertTrue(lowProduceRates.isEmpty(), String.format("%s: Unexpectedly low produce rate(s): %s", workloadResult.workload, lowProduceRates)));
            assertions.add(() -> assertTrue(lowConsumeRates.isEmpty(), String.format("%s: Unexpectedly low consume rate(s): %s", workloadResult.workload, lowConsumeRates)));
        }
        Assertions.assertAll(assertions);
    }
}
