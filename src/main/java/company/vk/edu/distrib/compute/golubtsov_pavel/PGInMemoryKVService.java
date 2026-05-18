package company.vk.edu.distrib.compute.golubtsov_pavel;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class PGInMemoryKVService implements KVService {
    public static final String ID_PARAM_PREFIX = "id=";
    private static final Logger log = LoggerFactory.getLogger(PGInMemoryKVService.class);
    private final Dao<byte[]> dao;
    private final HttpServer server;
    private final String selfEndpoint;
    private final ShardResolver shardResolver;
    private final HttpClient httpClient;

    public PGInMemoryKVService(int port,
                               Dao<byte[]> dao,
                               String selfEndpoint,
                               List<String> clusterEndpoints)
            throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        this.dao = dao;
        this.selfEndpoint = selfEndpoint;
        this.shardResolver = new ShardResolver(clusterEndpoints);
        this.httpClient = HttpClient.newHttpClient();
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new ErrorHttpHandler(http -> {
            final var method = http.getRequestMethod();
            if (Objects.equals("GET", method)) {
                http.sendResponseHeaders(200, 0);
            } else {
                http.sendResponseHeaders(405, 0);
            }
        }));

        server.createContext("/v0/entity", new ErrorHttpHandler(http -> {
            final var method = http.getRequestMethod();
            final var query = http.getRequestURI().getQuery();
            final var id = parseId(query);
            String owner = shardResolver.resolve(id);
            if (owner.equals(selfEndpoint)) {
                handleLocalEntityRequest(http, method, id);
            } else {
                proxyRequest(http, owner, id);
            }
        }));
    }

    private static String parseId(String query) {
        if ((query == null) || (!query.startsWith(ID_PARAM_PREFIX))) {
            throw new IllegalArgumentException("bad query");
        }
        String id = query.substring(ID_PARAM_PREFIX.length());
        if (id.isBlank()) {
            throw new IllegalArgumentException("empty id");
        }
        return id;
    }

    private void handleLocalEntityRequest(HttpExchange http, String method, String id) throws IOException {
        if (Objects.equals("GET", method)) {
            final var value = dao.get(id);
            http.sendResponseHeaders(200, value.length);
            http.getResponseBody().write(value);
        } else if (Objects.equals("PUT", method)) {
            byte[] body = http.getRequestBody().readAllBytes();
            dao.upsert(id, body);
            http.sendResponseHeaders(201, 0);
        } else if (Objects.equals("DELETE", method)) {
            dao.delete(id);
            http.sendResponseHeaders(202, 0);
        } else {
            http.sendResponseHeaders(405, 0);
        }
    }

    private void proxyRequest(HttpExchange http, String owner, String id) throws IOException {
        URI uri = URI.create(owner + "/v0/entity?id=" + id);
        String method = http.getRequestMethod();

        HttpRequest request;
        if (Objects.equals("GET", method)) {
            request = HttpRequest.newBuilder(uri).GET().build();
        } else if (Objects.equals("DELETE", method)) {
            request = HttpRequest.newBuilder(uri).DELETE().build();
        } else if (Objects.equals("PUT", method)) {
            byte[] body = http.getRequestBody().readAllBytes();
            request = HttpRequest.newBuilder(uri).PUT(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        } else {
            http.sendResponseHeaders(405, 0);
            return;
        }
        try {
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            byte[] responseBody = response.body();
            http.sendResponseHeaders(response.statusCode(), responseBody.length);
            http.getResponseBody().write(responseBody);
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new IOException(exp);
        }

    }

    @Override
    public void start() {
        log.info("Starting...");
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("Stopped");
    }

    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException exp) {
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException exp) {
                exchange.sendResponseHeaders(404,0);
            } catch (IOException exp) {
                exchange.sendResponseHeaders(500,0);
            }
            exchange.close();
        }
    }
}
