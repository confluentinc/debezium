package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Pattern;

import org.junit.Test;

public class DdlSkipPatternTest {

    private static final Pattern DDL_SKIP_PATTERN = Pattern.compile(
            "^(CREATE|ALTER|DROP)\\b.*?\\b(VIEW|FUNCTION|PROCEDURE|TRIGGER)\\b.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    @Test
    public void shouldMatchCreateView() {
        String ddl = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `employee_summary` AS SELECT CONCAT(first_name, ' ', last_name) AS full_name, department_id FROM employees";
        System.out.println(ddl);
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchAlterView() {
        String ddl = "ALTER ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `employee_summary` AS SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM employees";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchDropView() {
        String ddl = "DROP VIEW employee_summary";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchCreateFunction() {
        String ddl = "CREATE DEFINER=`root`@`localhost` FUNCTION `square`(x INT) RETURNS int\n" +
                "    DETERMINISTIC\n" +
                "RETURN x * x";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchDropFunction() {
        String ddl = "DROP FUNCTION square";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchCreateProcedure() {
        String ddl = "CREATE DEFINER=`root`@`localhost` PROCEDURE `greet_user`(IN username VARCHAR(50))\n" +
                "BEGIN\n" +
                "    SELECT CONCAT('Hello, ', username, '!') AS greeting;\n" +
                "END";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchDropProcedure() {
        String ddl = "drop procedure greet_user";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchCreateTrigger() {
        String ddl = "CREATE DEFINER=`root`@`localhost` TRIGGER after_user_insert\n" +
                "AFTER INSERT ON users\n" +
                "FOR EACH ROW\n" +
                "BEGIN\n" +
                "    INSERT INTO user_log (user_id, action, log_time)\n" +
                "    VALUES (NEW.id, 'User Created', NOW());\n" +
                "END";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldMatchDropTrigger() {
        String ddl = "DROP TRIGGER after_user_insert";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isTrue();
    }

    @Test
    public void shouldNotMatchCreateTable() {
        String ddl = "CREATE TABLE my_table (id INT PRIMARY KEY)";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isFalse();
    }

    @Test
    public void shouldNotMatchAlterTable() {
        String ddl = "ALTER TABLE my_table ADD COLUMN new_col VARCHAR(255)";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isFalse();
    }

    @Test
    public void shouldNotMatchDropTable() {
        String ddl = "DROP TABLE my_table";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isFalse();
    }

    @Test
    public void shouldNotMatchCreateDatabase() {
        String ddl = "CREATE DATABASE my_db";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isFalse();
    }

    @Test
    public void shouldNotMatchAlterDatabase() {
        String ddl = "ALTER DATABASE my_db CHARACTER SET utf8";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isFalse();
    }

    @Test
    public void shouldNotMatchDropDatabase() {
        String ddl = "DROP DATABASE my_db";
        assertThat(DDL_SKIP_PATTERN.matcher(ddl).matches()).isFalse();
    }
}