package org.bf2.performance;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class KafkaConnectionDetailsParameterResolver implements ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getParameterizedType().getTypeName().equals("java.util.List<org.bf2.performance.KafkaConnectionDetails>");
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        List<KafkaConnectionDetails> kafkas = new ArrayList<>();

        if (Files.exists(PerformanceEnvironment.PROVIDED_KAFKA_CLUSTERS_FILE)) {
            try {
                InputStream inputStream = Files.newInputStream(PerformanceEnvironment.PROVIDED_KAFKA_CLUSTERS_FILE);

                Yaml yaml = new Yaml(new Constructor(KafkaConnectionDetails.class));
                for (Object kafkaConnectionDetails : yaml.loadAll(inputStream)) {
                    kafkas.add((KafkaConnectionDetails) kafkaConnectionDetails);
                }
                if (kafkas.isEmpty()) {
                    throw new ParameterResolutionException(String.format("No Kafka connection details found in file: {}", PerformanceEnvironment.PROVIDED_KAFKA_CLUSTERS_FILE));
                }
            } catch (IOException e) {
                throw new ParameterResolutionException(String.format("Failed to read provided clusters file: %s", PerformanceEnvironment.PROVIDED_KAFKA_CLUSTERS_FILE), e);
            }
        } else {
            throw new ParameterResolutionException("No clusters file provided");
        }

        if (PerformanceEnvironment.MAX_KAFKA_INSTANCES > 0 && kafkas.size() > PerformanceEnvironment.MAX_KAFKA_INSTANCES) {
            kafkas = kafkas.subList(0, PerformanceEnvironment.MAX_KAFKA_INSTANCES);
        }
        return kafkas;
    }

}
