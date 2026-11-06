package jobs.store;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

/**
 * Migrates the application's own schema. It shares a database with Agenor's directory, so two
 * settings matter; the order of the two migrations does not.
 *
 * <ul>
 *   <li>a history table of our own, so that our {@code V1} is never confused with another
 *       schema's {@code V1} in Flyway's default history table;
 *   <li>{@code baselineVersion("0")}: when the directory is already there the schema is not
 *       empty, so Flyway has to baseline, and its default baseline is version 1, which would
 *       mark our {@code V1} as already applied and skip it without a word.
 * </ul>
 */
public final class JobSchema {

    private JobSchema() {}

    public static void migrate(DataSource dataSource) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration/jobs")
                .table("jobs_schema_history")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate();
    }
}
