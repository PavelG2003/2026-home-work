package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaAuditService implements AuditService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAuditService.class);
    private static final String AUDIT_TOPIC = "audit";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final List<AuditEvent> entries = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean();

    private KafkaConsumer<String, String> consumer;
    private Thread consumerThread;

    public KafkaAuditService(String bootstrapServers, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        consumer = new KafkaConsumer<>(consumerProperties());
        consumer.subscribe(List.of(AUDIT_TOPIC));
        consumerThread = new Thread(this::pollLoop, "audit-consumer-" + consumerGroupId);
        consumerThread.start();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        consumer.wakeup();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.copyOf(entries);
    }

    private void pollLoop() {
        try (KafkaConsumer<String, String> localConsumer = consumer) {
            while (running.get()) {
                var records = localConsumer.poll(POLL_TIMEOUT);
                for (var record : records) {
                    entries.add(AuditEventUtils.decode(record.value()));
                }
                if (!records.isEmpty()) {
                    localConsumer.commitSync();
                }
            }
            localConsumer.commitSync();
        } catch (WakeupException e) {
            if (running.get()) {
                LOG.warn("Audit consumer was interrupted unexpectedly", e);
            }
        } catch (RuntimeException e) {
            LOG.error("Audit consumer failed", e);
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
