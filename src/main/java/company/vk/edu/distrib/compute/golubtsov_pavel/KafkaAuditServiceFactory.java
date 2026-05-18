package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

public class KafkaAuditServiceFactory extends AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) {
        return new KafkaAuditService(bootstrapServers, consumerGroupId);
    }
}
