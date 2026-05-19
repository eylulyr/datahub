package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgCron;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgCron.Admin;
import java.sql.SQLException;
import org.testng.annotations.Test;

public class PgCronAdminConnectionsTest {

  @Test
  public void open_requiresAdminJdbcUrl() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    PgCron pgCron = new PgCron();
    pgCron.setAdmin(new Admin());
    props.setPgCron(pgCron);

    IllegalStateException ex =
        expectThrows(IllegalStateException.class, () -> PgCronAdminConnections.open(props));
    assertEquals(
        ex.getMessage(),
        "postgres.pgCron.admin.jdbcUrl must be configured when pg_cron SqlSetup runs.");
  }

  @Test
  public void open_rejectsBlankJdbcUrl() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    PgCron pgCron = new PgCron();
    Admin admin = new Admin();
    admin.setJdbcUrl("   ");
    pgCron.setAdmin(admin);
    props.setPgCron(pgCron);

    IllegalStateException ex =
        expectThrows(IllegalStateException.class, () -> PgCronAdminConnections.open(props));
    assertEquals(
        ex.getMessage(),
        "postgres.pgCron.admin.jdbcUrl must be configured when pg_cron SqlSetup runs.");
  }

  @Test
  public void open_usesPasswordAuthWhenIamDisabled() throws SQLException {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    PgCron pgCron = new PgCron();
    Admin admin = new Admin();
    admin.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
    admin.setUsername("cron");
    admin.setPassword("secret");
    admin.setDriver("org.postgresql.Driver");
    pgCron.setAdmin(admin);
    props.setPgCron(pgCron);

    expectThrows(SQLException.class, () -> PgCronAdminConnections.open(props));
  }
}
