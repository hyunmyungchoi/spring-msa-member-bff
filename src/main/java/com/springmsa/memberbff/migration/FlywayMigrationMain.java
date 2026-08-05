package com.springmsa.memberbff.migration;

import org.flywaydb.core.Flyway;

import java.util.Map;

public final class FlywayMigrationMain {

    static final String SCHEMA = "member_bff";

    private FlywayMigrationMain() {
    }

    public static void main(String[] args) {
        if (args.length > 0 && "repair".equalsIgnoreCase(args[0])) {
            repair(System.getenv());
            System.out.printf("Flyway repair completed: schema=%s%n", SCHEMA);
            return;
        }
        migrate(System.getenv());
        System.out.printf("Flyway migration completed: schema=%s%n", SCHEMA);
    }

    static void migrate(Map<String, String> environment) {
        configured(environment).migrate();
    }

    static void repair(Map<String, String> environment) {
        configured(environment).repair();
    }

    private static Flyway configured(Map<String, String> environment) {
        return Flyway.configure()
                .dataSource(
                        required(environment, "SPRING_DATASOURCE_URL"),
                        required(environment, "SPRING_DATASOURCE_USERNAME"),
                        required(environment, "SPRING_DATASOURCE_PASSWORD"))
                .defaultSchema(SCHEMA)
                .schemas(SCHEMA)
                .createSchemas(true)
                .locations("classpath:db/migration")
                .validateOnMigrate(true)
                .load();
    }

    private static String required(Map<String, String> environment, String name) {
        String value = environment.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required for the Flyway migration task");
        }
        return value;
    }
}
