package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

public class AuditableInMemoryKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) {
        return new AuditableInMemoryKVService(port);
    }
}
