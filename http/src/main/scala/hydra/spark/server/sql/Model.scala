package hydra.spark.server.sql

object PostgresConfigsComponent extends JobConfigsComponent with PgPersistence

object PostgresJobsComponent extends JobsComponent with PgPersistence

object PostgresBinariesComponent extends BinariesComponent with PgPersistence

