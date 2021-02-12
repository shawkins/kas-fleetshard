package org.bf2.operator.controllers;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.Context;
import io.javaoperatorsdk.operator.api.Controller;
import io.javaoperatorsdk.operator.api.DeleteControl;
import io.javaoperatorsdk.operator.api.ResourceController;
import io.javaoperatorsdk.operator.api.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.EventSourceManager;
import io.javaoperatorsdk.operator.processing.event.internal.CustomResourceEvent;
import io.strimzi.api.kafka.model.Kafka;
import org.bf2.operator.events.DeploymentEvent;
import org.bf2.operator.events.DeploymentEventSource;
import org.bf2.operator.events.KafkaEvent;
import org.bf2.operator.events.KafkaEventSource;
import org.bf2.operator.ConditionUtils;
import org.bf2.operator.operands.KafkaInstance;
import org.bf2.operator.resources.v1alpha1.ManagedKafka;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCapacityBuilder;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaStatus;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaStatusBuilder;
import org.bf2.operator.resources.v1alpha1.VersionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Controller
public class ManagedKafkaController implements ResourceController<ManagedKafka> {

    private static final Logger log = LoggerFactory.getLogger(ManagedKafkaController.class);

    @Inject
    KafkaEventSource kafkaEventSource;

    @Inject
    DeploymentEventSource deploymentEventSource;

    @Inject
    KafkaInstance kafkaInstance;

    @Override
    public DeleteControl deleteResource(ManagedKafka managedKafka, Context<ManagedKafka> context) {
        log.info("Deleting Kafka instance {}", managedKafka.getMetadata().getName());
        kafkaInstance.delete(managedKafka, context);
        return DeleteControl.DEFAULT_DELETE;
    }

    @Override
    public UpdateControl<ManagedKafka> createOrUpdateResource(ManagedKafka managedKafka, Context<ManagedKafka> context) {

        Optional<CustomResourceEvent> latestManagedKafkaEvent =
                context.getEvents().getLatestOfType(CustomResourceEvent.class);

        if (latestManagedKafkaEvent.isPresent()) {
            kafkaInstance.createOrUpdate(managedKafka);
        }

        Optional<KafkaEvent> latestKafkaEvent =
                context.getEvents().getLatestOfType(KafkaEvent.class);
        if (latestKafkaEvent.isPresent()) {
            Kafka kafka = latestKafkaEvent.get().getKafka();
            log.info("Kafka resource {}/{} is changed", kafka.getMetadata().getNamespace(), kafka.getMetadata().getName());
            updateManagedKafkaStatus(managedKafka);
            kafkaInstance.createOrUpdate(managedKafka);
            return UpdateControl.updateStatusSubResource(managedKafka);
        }

        Optional<DeploymentEvent> latestDeploymentEvent =
                context.getEvents().getLatestOfType(DeploymentEvent.class);
        if (latestDeploymentEvent.isPresent()) {
            Deployment deployment = latestDeploymentEvent.get().getDeployment();
            log.info("Deployment resource {}/{} is changed", deployment.getMetadata().getNamespace(), deployment.getMetadata().getName());
            updateManagedKafkaStatus(managedKafka);
            kafkaInstance.createOrUpdate(managedKafka);
            return UpdateControl.updateStatusSubResource(managedKafka);
        }

        return UpdateControl.noUpdate();
    }

    @Override
    public void init(EventSourceManager eventSourceManager) {
        log.info("init");
        eventSourceManager.registerEventSource("kafka-event-source", kafkaEventSource);
        eventSourceManager.registerEventSource("deployment-event-source", deploymentEventSource);
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
                .withConditions(Collections.emptyList())
                .build());
        managedKafka.setStatus(status);

        List<ManagedKafkaCondition> managedKafkaConditions = managedKafka.getStatus().getConditions();
        Optional<ManagedKafkaCondition> optInstalling =
                ConditionUtils.findManagedKafkaCondition(managedKafkaConditions, ManagedKafkaCondition.Type.Installing);
        Optional<ManagedKafkaCondition> optReady =
                ConditionUtils.findManagedKafkaCondition(managedKafkaConditions, ManagedKafkaCondition.Type.Ready);
        Optional<ManagedKafkaCondition> optError =
                ConditionUtils.findManagedKafkaCondition(managedKafkaConditions, ManagedKafkaCondition.Type.Error);

        if (kafkaInstance.isInstalling(managedKafka)) {
            if (optInstalling.isPresent()) {
                ConditionUtils.updateConditionStatus(optInstalling.get(), "True");
            } else {
                ManagedKafkaCondition installing = ConditionUtils.buildCondition(ManagedKafkaCondition.Type.Installing, "True");
                managedKafkaConditions.add(installing);
            }
            // TODO: should we really have even Ready and Error condition type as "False" while installing, so creating them if not exist?
            if (optReady.isPresent()) {
                ConditionUtils.updateConditionStatus(optReady.get(), "False");
            }
            if (optError.isPresent()) {
                ConditionUtils.updateConditionStatus(optError.get(), "False");
            }
        } else if (kafkaInstance.isReady(managedKafka)) {
            if (optInstalling.isPresent()) {
                ConditionUtils.updateConditionStatus(optInstalling.get(), "False");
            }
            if (optReady.isPresent()) {
                ConditionUtils.updateConditionStatus(optReady.get(), "True");
            } else {
                ManagedKafkaCondition ready = ConditionUtils.buildCondition(ManagedKafkaCondition.Type.Ready, "True");
                managedKafkaConditions.add(ready);
            }
            if (optError.isPresent()) {
                ConditionUtils.updateConditionStatus(optError.get(), "False");
            }

            // TODO: just reflecting for now what was defined in the spec
            managedKafka.getStatus().setCapacity(new ManagedKafkaCapacityBuilder(managedKafka.getSpec().getCapacity()).build());
            managedKafka.getStatus().setVersions(new VersionsBuilder(managedKafka.getSpec().getVersions()).build());

        } else if (kafkaInstance.isError(managedKafka)) {
            if (optInstalling.isPresent()) {
                ConditionUtils.updateConditionStatus(optInstalling.get(), "False");
            }
            if (optReady.isPresent()) {
                ConditionUtils.updateConditionStatus(optReady.get(), "False");
            }
            if (optError.isPresent()) {
                ConditionUtils.updateConditionStatus(optError.get(), "True");
            } else {
                ManagedKafkaCondition error = ConditionUtils.buildCondition(ManagedKafkaCondition.Type.Error, "True");
                managedKafkaConditions.add(error);
            }
        }
    }
}
