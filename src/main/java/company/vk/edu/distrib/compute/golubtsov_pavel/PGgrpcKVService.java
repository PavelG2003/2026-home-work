package company.vk.edu.distrib.compute.golubtsov_pavel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class PGgrpcKVService extends PGInMemoryKVService {
    public PGgrpcKVService(int port) throws IOException {
        super(
                port,
                new PGFileDao(Path.of("PGData", String.valueOf(port))),
                "http://localhost:" + port,
                List.of("http://localhost:" + port)
        );
    }
}
