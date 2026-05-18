package company.vk.edu.distrib.compute.golubtsov_pavel;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class AuditableInMemoryKVService implements AuditableKVService {
    private static final Logger LOG = LoggerFactory.getLogger(AuditableInMemoryKVService.class);
    private static final int EMPTY_RESPONSE_LENGTH = -1;
    private static final String AUDIT_TOPIC = "audit";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();
    private final HttpServer server;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicBoolean async = new AtomicBoolean(true);

    private String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private KafkaProducer<String, String> producer;

    public AuditableInMemoryKVService(int port) {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create HTTP server on port " + port, e);
        }
        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
        server.setExecutor(executorService);
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void setAsync(boolean enabled) {
        async.set(enabled);
    }

    @Override
    public void start() {
        producer = new KafkaProducer<>(producerProperties());
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        executorService.shutdown();
        KafkaProducer<String, String> localProducer = producer;
        if (localProducer != null) {
            localProducer.close();
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if ("GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, EMPTY_RESPONSE_LENGTH);
            } else {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, EMPTY_RESPONSE_LENGTH);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                String id = extractId(exchange.getRequestURI().getRawQuery());
                String method = exchange.getRequestMethod();
                sendAuditEvent(new AuditEvent(method, id, System.currentTimeMillis()));

                switch (method) {
                    case "GET" -> handleGet(exchange, id);
                    case "PUT" -> handlePut(exchange, id);
                    case "DELETE" -> handleDelete(exchange, id);
                    default -> exchange.sendResponseHeaders(
                            HttpURLConnection.HTTP_BAD_METHOD,
                            EMPTY_RESPONSE_LENGTH
                    );
                }
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, EMPTY_RESPONSE_LENGTH);
            } catch (RuntimeException e) {
                LOG.error("Failed to process entity request", e);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, EMPTY_RESPONSE_LENGTH);
            }
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = storage.get(id);
        if (value == null) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, EMPTY_RESPONSE_LENGTH);
            return;
        }

        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, value.length);
        try (OutputStream responseBody = exchange.getResponseBody()) {
            responseBody.write(value);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        storage.put(id, exchange.getRequestBody().readAllBytes());
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, EMPTY_RESPONSE_LENGTH);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        storage.remove(id);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, EMPTY_RESPONSE_LENGTH);
    }

    private String extractId(String query) {
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Missing query parameter id");
        }

        return Arrays.stream(query.split("&"))
                .filter(parameter -> parameter.startsWith("id="))
                .findFirst()
                .map(parameter -> parameter.substring("id=".length()))
                .map(value -> URLDecoder.decode(value, StandardCharsets.UTF_8))
                .filter(value -> !value.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Missing query parameter id"));
    }

    private void sendAuditEvent(AuditEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                AUDIT_TOPIC,
                event.id(),
                AuditEventUtils.encode(event)
        );
        if (async.get()) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn("Failed to send audit event", exception);
                }
            });
        } else {
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while sending audit event", e);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to send audit event", e);
            }
        }
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }
}
