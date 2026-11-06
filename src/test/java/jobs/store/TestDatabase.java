package jobs.store;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jobs.testsupport.Infra;

import java.sql.SQLException;
import java.util.UUID;

/**
 * Pools on databases created inside the PostgreSQL that {@link Infra} already runs.
 *
 * <p>Not a container of its own: after a minute without Docker calls, a second {@code start()}
 * reuses a keep-alive connection to the socket that has been closed, and fails with
 * {@code Broken pipe}. One container per JVM avoids a second start altogether, and is faster.
 */
public final class TestDatabase {

    public static final String USER = Infra.JDBC_USER;
    public static final String PASSWORD = Infra.JDBC_PASSWORD;

    private TestDatabase() {}

    /** JDBC URL of {@code database} on the shared container. */
    public static String jdbcUrl(String database) {
        return Infra.jdbcUrl().replaceFirst("/[^/]+$", "/" + database);
    }

    /** A pool on a database of its own, so no test sees another's tables. */
    public static HikariDataSource freshDatabase() {
        var name = "t_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        try (var admin = pool("postgres", 1); var conn = admin.getConnection();
             var st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + name);
        } catch (SQLException e) {
            throw new IllegalStateException("cannot create database " + name, e);
        }
        return pool(name, 16);
    }

    private static HikariDataSource pool(String database, int size) {
        var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl(database));
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(size);
        return new HikariDataSource(config);
    }
}
