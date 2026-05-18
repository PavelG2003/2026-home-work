package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PGCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(PGCluster.class);
    private final List<String> endpoints;
    private final Map<String, KVService> runningNodes;
    private final Map<String, Integer> endpointToPort;

    public PGCluster(List<Integer> ports) {
        List<Integer> clusterPorts = List.copyOf(ports);
        this.endpoints = clusterPorts.stream()
                .map(port -> "http://localhost:" + port)
                .toList();
        this.runningNodes = new HashMap<>();
        this.endpointToPort = new HashMap<>();

        for (int i = 0; i < clusterPorts.size(); i++) {
            endpointToPort.put(this.endpoints.get(i), clusterPorts.get(i));
        }
    }

    private KVService createNode(int port, String endpoint) throws IOException {
        PGFileDao dao = new PGFileDao(Path.of("PGData", String.valueOf(port)));
        return new PGInMemoryKVService(port, dao, endpoint, endpoints);
    }

    @Override
    public void start() {
        try {
            for (String endpoint : endpoints) {
                start(endpoint);
            }
        } catch (RuntimeException e) {
            stop();
            throw e;
        }
    }

    @Override
    public void start(String endpoint) {
        if (runningNodes.containsKey(endpoint)) {
            log.info("node is already start");
            return;
        }
        Integer port = endpointToPort.get(endpoint);
        if (port == null) {
            throw new IllegalArgumentException("port is null");
        }
        try {
            KVService node = createNode(port, endpoint);
            node.start();
            runningNodes.put(endpoint, node);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start node " + endpoint, e);
        }
    }

    @Override
    public void stop() {
        for (String endpoint : endpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        if (!runningNodes.containsKey(endpoint)) {
            log.info("node is not start");
            return;
        }
        KVService node = runningNodes.get(endpoint);
        node.stop();
        runningNodes.remove(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }
}
