package me.smecsia.cassajem;

import me.smecsia.cassajem.service.Connection;
import me.smecsia.cassajem.service.EmbeddedCassandraService;
import me.smecsia.cassajem.util.ArrayUtil;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CassajemTest {

    private static Logger logger = LoggerFactory.getLogger(CassajemTest.class);
    private static EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService("/cassandra-test.yaml");
    private static Connection connection = null;

    public static void cleanupStorage(String... locations) {
        System.out.println("Performing the storage cleanup...");
        try {
            for (String tmpLocation : locations) {
                File file = new File(tmpLocation);
                if (file.isDirectory() && file.exists()) {
                    FileUtils.deleteRecursive(file);
                }
            }
            System.out.println("Storage cleaned up!");
        } catch (Exception e) {
            System.out.println("Storage cleanup error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected EmbeddedCassandraService getService() {
        return embeddedCassandraService;
    }

    protected Connection getConnection() {
        return connection;
    }

    static {
        try {
            cleanupStorage(
                    ArrayUtil.add(DatabaseDescriptor.getAllDataFileLocations(),
                            DatabaseDescriptor.getCommitLogLocation(),
                            DatabaseDescriptor.getSavedCachesLocation())
            );
            embeddedCassandraService.init();
            connection = embeddedCassandraService.getConnection();
            connection.dropKeySpaceIfExists();
            connection.createKeySpaceIfNotExist();
        } catch (Exception e) {
            logger.error("Failed to initialize internal storage: " + e.getMessage());
            System.exit(-1);
        }
    }
}
