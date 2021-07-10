package org.bf2.operator.managers;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.openshift.api.model.operator.v1.IngressController;
import io.fabric8.openshift.client.OpenShiftClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kubernetes.client.KubernetesServerTestResource;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTestResource(KubernetesServerTestResource.class)
@QuarkusTest
public class IngressControllerManagerTest {

    @Inject
    IngressControllerManager ingressControllerManager;

    @Inject
    OpenShiftClient openShiftClient;

    @Test
    public void testIngressControllerCreationWithNoZones() {
        ingressControllerManager.reconcileIngressControllers();

        List<IngressController> ingressControllers = openShiftClient.operator().ingressControllers().inNamespace(IngressControllerManager.INGRESS_OPERATOR_NAMESPACE).list().getItems();
        assertEquals(1, ingressControllers.size(), "Expected only one IngressController");
        assertEquals("kas", ingressControllers.get(0).getMetadata().getName(), "Expected the IngressController to be named kas");
        assertEquals(0, ingressControllers.get(0).getSpec().getReplicas(), "Expected 0 replicas because there are 0 nodes");
    }

    @Test
    public void testIngressControllerCreationWith3Zones() {

        IntStream.range(0, 3).forEach(i -> {
            Node node = new NodeBuilder()
                    .editOrNewMetadata()
                        .withName("z"+i)
                        .withLabels(Map.of(IngressControllerManager.WORKER_NODE_LABEL, "", IngressControllerManager.TOPOLOGY_KEY, "zone"+i))
                    .endMetadata()
                    .build();
            openShiftClient.nodes().create(node);
        });

        ingressControllerManager.reconcileIngressControllers();

        List<IngressController> ingressControllers = openShiftClient.operator().ingressControllers().inNamespace(IngressControllerManager.INGRESS_OPERATOR_NAMESPACE).list().getItems();
        assertEquals(4, ingressControllers.size(), "Expected 4 IngressControllers: one per zone, and one multi-zone");
    }
}
