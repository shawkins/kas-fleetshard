package org.bf2.operator.controllers;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.javaoperatorsdk.operator.api.Context;
import io.javaoperatorsdk.operator.api.Controller;
import io.javaoperatorsdk.operator.api.DeleteControl;
import io.javaoperatorsdk.operator.api.ResourceController;
import io.javaoperatorsdk.operator.api.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.EventSourceManager;
import io.javaoperatorsdk.operator.processing.event.internal.CustomResourceEvent;
import io.strimzi.api.kafka.model.Kafka;

import org.bf2.common.ConditionUtils;
import org.bf2.operator.events.ResourceEvent;
import org.bf2.operator.events.ResourceEventSource;
import org.bf2.operator.operands.KafkaInstance;
import org.bf2.operator.resources.v1alpha1.ManagedKafka;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCapacityBuilder;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition.Reason;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition.Status;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaStatus;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaStatusBuilder;
import org.bf2.operator.resources.v1alpha1.VersionsBuilder;
import org.jboss.logging.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Controller
public class ManagedKafkaController implements ResourceController<ManagedKafka> {

    @Inject
    Logger log;

    @Inject
    KubernetesClient kubernetesClient;

    @Inject
    ResourceEventSource.KafkaEventSource kafkaEventSource;

    @Inject
    ResourceEventSource.DeploymentEventSource deploymentEventSource;

    @Inject
    ResourceEventSource.ServiceEventSource serviceEventSource;

    @Inject
    ResourceEventSource.ConfigMapEventSource configMapEventSource;

    @Inject
    ResourceEventSource.SecretEventSource secretEventSource;

    @Inject
    ResourceEventSource.RouteEventSource routeEventSource;

    @Inject
    KafkaInstance kafkaInstance;

    @Override
    public DeleteControl deleteResource(ManagedKafka managedKafka, Context<ManagedKafka> context) {
        log.infof("Deleting Kafka instance %s/%s", managedKafka.getMetadata().getNamespace(), managedKafka.getMetadata().getName());
        kafkaInstance.delete(managedKafka, context);
        return DeleteControl.DEFAULT_DELETE;
    }

    public void handleUpdate(ManagedKafka managedKafka, Context<ManagedKafka> context) {
        // if the ManagedKafka resource is "marked" as to be deleted
        if (managedKafka.getSpec().isDeleted()) {
            // check that it's actually not deleted yet, so operands are gone
            if (!kafkaInstance.isDeleted(managedKafka)) {
                log.infof("Deleting Kafka instance %s/%s", managedKafka.getMetadata().getNamespace(), managedKafka.getMetadata().getName());
                kafkaInstance.delete(managedKafka, context);
            }
        } else {
            kafkaInstance.createOrUpdate(managedKafka);
        }
    }

    @Override
    public UpdateControl<ManagedKafka> createOrUpdateResource(ManagedKafka managedKafka, Context<ManagedKafka> context) {

        Optional<CustomResourceEvent> latestManagedKafkaEvent =
                context.getEvents().getLatestOfType(CustomResourceEvent.class);

        if (latestManagedKafkaEvent.isPresent()) {
            handleUpdate(managedKafka, context);
        }

        Optional<ResourceEvent.KafkaEvent> latestKafkaEvent =
                context.getEvents().getLatestOfType(ResourceEvent.KafkaEvent.class);
        if (latestKafkaEvent.isPresent()) {
            Kafka kafka = latestKafkaEvent.get().getResource();
            log.infof("Kafka resource %s/%s is changed", kafka.getMetadata().getNamespace(), kafka.getMetadata().getName());
            updateManagedKafkaStatus(managedKafka);
            handleUpdate(managedKafka, context);
            return UpdateControl.updateStatusSubResource(managedKafka);
        }

        Optional<ResourceEvent.DeploymentEvent> latestDeploymentEvent =
                context.getEvents().getLatestOfType(ResourceEvent.DeploymentEvent.class);
        if (latestDeploymentEvent.isPresent()) {
            Deployment deployment = latestDeploymentEvent.get().getResource();
            log.infof("Deployment resource %s/%s is changed", deployment.getMetadata().getNamespace(), deployment.getMetadata().getName());
            updateManagedKafkaStatus(managedKafka);
            handleUpdate(managedKafka, context);
            return UpdateControl.updateStatusSubResource(managedKafka);
        }

        Optional<ResourceEvent.ServiceEvent> latestServiceEvent =
                context.getEvents().getLatestOfType(ResourceEvent.ServiceEvent.class);
        if (latestServiceEvent.isPresent()) {
            Service service = latestServiceEvent.get().getResource();
            log.infof("Service resource %s/%s is changed", service.getMetadata().getNamespace(), service.getMetadata().getName());
            handleUpdate(managedKafka, context);
            return UpdateControl.noUpdate();
        }

        Optional<ResourceEvent.ConfigMapEvent> latestConfigMapEvent =
                context.getEvents().getLatestOfType(ResourceEvent.ConfigMapEvent.class);
        if (latestConfigMapEvent.isPresent()) {
            ConfigMap configMap = latestConfigMapEvent.get().getResource();
            log.infof("ConfigMap resource %s/%s is changed", configMap.getMetadata().getNamespace(), configMap.getMetadata().getName());
            handleUpdate(managedKafka, context);
            return UpdateControl.noUpdate();
        }

        Optional<ResourceEvent.SecretEvent> latestSecretEvent =
                context.getEvents().getLatestOfType(ResourceEvent.SecretEvent.class);
        if (latestSecretEvent.isPresent()) {
            Secret secret = latestSecretEvent.get().getResource();
            log.infof("Secret resource %s/%s is changed", secret.getMetadata().getNamespace(), secret.getMetadata().getName());
            handleUpdate(managedKafka, context);
            return UpdateControl.noUpdate();
        }

        Optional<ResourceEvent.RouteEvent> latestRouteEvent =
                context.getEvents().getLatestOfType(ResourceEvent.RouteEvent.class);
        if (latestRouteEvent.isPresent()) {
            Route route = latestRouteEvent.get().getResource();
            log.infof("Route resource %s/%s is changed", route.getMetadata().getNamespace(), route.getMetadata().getName());
            handleUpdate(managedKafka,context);
            return UpdateControl.noUpdate();
        }

        return UpdateControl.noUpdate();
    }

    @Override
    public void init(EventSourceManager eventSourceManager) {
        log.info("init");
        eventSourceManager.registerEventSource("kafka-event-source", kafkaEventSource);
        eventSourceManager.registerEventSource("deployment-event-source", deploymentEventSource);
        eventSourceManager.registerEventSource("service-event-source", serviceEventSource);
        eventSourceManager.registerEventSource("configmap-event-source", configMapEventSource);
        eventSourceManager.registerEventSource("secret-event-source", secretEventSource);
        if (kubernetesClient.isAdaptable(OpenShiftClient.class)) {
            eventSourceManager.registerEventSource("route-event-source", routeEventSource);
        }
    }

    /**
     * Extract from the current KafkaInstance overall status (Kafka, Canary and AdminServer)
     * a corresponding list of ManagedKafkaCondition(s) to set on the ManagedKafka status
     *
     * @param managedKafka ManagedKafka instance
     */
    private void updateManagedKafkaStatus(ManagedKafka managedKafka) {
        // add status if not already available on the ManagedKafka resource
        ManagedKafkaStatus status = Objects.requireNonNullElse(managedKafka.getStatus(),
                new ManagedKafkaStatusBuilder()
                .build());
        status.setUpdatedTimestamp(ConditionUtils.iso8601Now());
        managedKafka.setStatus(status);

        // add conditions if not already available
        List<ManagedKafkaCondition> managedKafkaConditions = managedKafka.getStatus().getConditions();
        if (managedKafkaConditions == null) {
            managedKafkaConditions = new ArrayList<>();
            status.setConditions(managedKafkaConditions);
        }
        Optional<ManagedKafkaCondition> optReady =
                ConditionUtils.findManagedKafkaCondition(managedKafkaConditions, ManagedKafkaCondition.Type.Ready);

        ManagedKafkaCondition ready = null;

        if (optReady.isPresent()) {
            ready = optReady.get();
        } else {
            ready = ConditionUtils.buildCondition(ManagedKafkaCondition.Type.Ready, Status.Unknown);
            managedKafkaConditions.add(ready);
        }

        if (managedKafka.getSpec().isDeleted() && kafkaInstance.isDeleted(managedKafka)) {
            ConditionUtils.updateConditionStatus(ready, Status.False, Reason.Deleted);
        } else if (kafkaInstance.isInstalling(managedKafka)) {
            ConditionUtils.updateConditionStatus(ready, Status.False, Reason.Installing);
        } else if (kafkaInstance.isReady(managedKafka)) {
            ConditionUtils.updateConditionStatus(ready, Status.True, null);

            // TODO: just reflecting for now what was defined in the spec
            managedKafka.getStatus().setCapacity(new ManagedKafkaCapacityBuilder(managedKafka.getSpec().getCapacity()).build());
            managedKafka.getStatus().setVersions(new VersionsBuilder(managedKafka.getSpec().getVersions()).build());
            managedKafka.getStatus().setAdminServerURI(kafkaInstance.getAdminServer().Uri(managedKafka));

        } else if (kafkaInstance.isError(managedKafka)) {
            ConditionUtils.updateConditionStatus(ready, Status.False, Reason.Error);
        } else {
            ConditionUtils.updateConditionStatus(ready, Status.Unknown, null);
        }
    }
}
