package org.bf2.systemtest.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.operator.resources.v1alpha1.ManagedKafka;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition;
import org.bf2.systemtest.framework.TimeoutBudget;
import org.bf2.systemtest.framework.resource.ManagedKafkaResourceType;
import org.bf2.systemtest.operator.FleetShardOperatorManager;
import org.bf2.systemtest.operator.StrimziOperatorManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;

public class RecoveryST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    @BeforeAll
    void deploy() throws Exception {
        StrimziOperatorManager.installStrimzi(kube);
        FleetShardOperatorManager.deployFleetShardOperator(kube);
    }

    @AfterAll
    void clean() throws InterruptedException {
        FleetShardOperatorManager.deleteFleetShard(kube);
        StrimziOperatorManager.uninstallStrimziClusterWideResources(kube);
    }

    @Test
    void testDeleteDeployedResources(ExtensionContext extensionContext) {
        String mkAppName = "mk-test-resource-recovery";

        var kafkacli = kube.client().customResources(Kafka.class, KafkaList.class);

        LOGGER.info("Create namespace");
        resourceManager.createResource(extensionContext,
                new NamespaceBuilder().withNewMetadata().withName(mkAppName).endMetadata().build());

        LOGGER.info("Create managedkafka");
        ManagedKafka mk = ManagedKafkaResourceType.getDefault(mkAppName, mkAppName);

        resourceManager.createResource(extensionContext, mk);

        LOGGER.info("Delete resources in namespace {}", mkAppName);
        kube.client()
                .apps()
                .deployments()
                .inNamespace(mkAppName)
                .withLabel("app.kubernetes.io/managed-by", "kas-fleetshard-operator")
                .delete();
        kafkacli.inNamespace(mkAppName).withLabel("app.kubernetes.io/managed-by", "kas-fleetshard-operator").delete();

        assertTrue(resourceManager.waitResourceCondition(mk, m -> ManagedKafkaResourceType.hasConditionStatus(m,
                ManagedKafkaCondition.Type.Ready, ManagedKafkaCondition.Status.False)));

        assertTrue(resourceManager.waitResourceCondition(mk,
                m -> ManagedKafkaResourceType.hasConditionStatus(m, ManagedKafkaCondition.Type.Ready,
                        ManagedKafkaCondition.Status.True),
                TimeoutBudget.ofDuration(Duration.ofMinutes(15))));

        assertNotNull(ManagedKafkaResourceType.getOperation().inNamespace(mkAppName).withName(mkAppName).get());
        assertNotNull(kafkacli.inNamespace(mkAppName).withName(mkAppName).get());
        assertTrue(kube.client().pods().inNamespace(mkAppName).list().getItems().size() > 0);
        assertEquals("Running", ManagedKafkaResourceType.getCanaryPod(mk).getStatus().getPhase());
        assertEquals("Running", ManagedKafkaResourceType.getAdminApiPod(mk).getStatus().getPhase());
        assertEquals(3, ManagedKafkaResourceType.getKafkaPods(mk).size());
        assertEquals(3, ManagedKafkaResourceType.getZookeeperPods(mk).size());
    }
}
