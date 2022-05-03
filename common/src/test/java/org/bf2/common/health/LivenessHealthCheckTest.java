package org.bf2.common.health;

import org.bf2.common.ResourceInformerFactory;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LivenessHealthCheckTest {

    @Test public void testLiveness() {
        // check the dummy behavior - will need to be updated when there is something real
        LivenessHealthCheck livenessHealthCheck = new LivenessHealthCheck();
        livenessHealthCheck.resourceInformerFactory = Mockito.mock(ResourceInformerFactory.class);
        assertEquals(HealthCheckResponse.Status.DOWN, livenessHealthCheck.call().getStatus());
        Mockito.when(livenessHealthCheck.resourceInformerFactory.allInformersWatching()).thenReturn(true);
        assertEquals(HealthCheckResponse.Status.UP, livenessHealthCheck.call().getStatus());
    }

}
