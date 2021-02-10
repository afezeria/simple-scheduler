package github.afezeria.simplescheduler.sql

import com.p6spy.engine.spy.P6DataSource
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import github.afezeria.simplescheduler.KPostgreSQLContainer
import github.afezeria.simplescheduler.execute
import io.kotest.matchers.shouldBe
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import java.time.Duration
import java.time.LocalDateTime
import javax.sql.DataSource

/**
 * @author afezeria
 */
class CronTest {
    companion object {
        lateinit var container: KPostgreSQLContainer
        lateinit var dataSource: DataSource

        @JvmStatic
        @BeforeAll
        fun before() {
            container = KPostgreSQLContainer("postgres:12.5-alpine")
                .withFileSystemBind(
                    "sql", "/docker-entrypoint-initdb.d",
                    BindMode.READ_ONLY
                )
                .withExposedPorts(5432)
                .waitingFor(
                    HostPortWaitStrategy()
                        .withStartupTimeout(Duration.ofSeconds(10))
                )
            container.start()
            dataSource = P6DataSource(
                HikariDataSource(HikariConfig().apply {
//                    jdbcUrl =
//                        "jdbc:postgresql://localhost:5432/test"
//                    username = "test"
//                    password = "123456"
                    jdbcUrl = container.jdbcUrl
                    username = container.username
                    password = container.password
                })
            )
        }

        @AfterAll
        @JvmStatic
        fun afterAll() {
            if (this::container.isInitialized) {
                container.use { }
            }
        }

        fun sql(
            @Language("sql") sql: String,
            vararg params: Any?
        ): MutableList<MutableMap<String, Any?>> {
            return dataSource.connection.use {
                it.execute(sql, *params)
            }
        }

    }

    @Test
    fun `cron expr test`() {
        sql(
            "select simples_f_get_next_execution_time('* * * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 0, 1)
        sql(
            "select simples_f_get_next_execution_time('10-20 * * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 0, 10)
        sql(
            "select simples_f_get_next_execution_time('* 5,6,7 * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* * 2/3 * ?', '2020-01-03 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 5, 0, 0)
        sql(
            "select simples_f_get_next_execution_time('* * ? * 2', '2021-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 1, 5, 0, 0)
        sql(
            "select simples_f_get_next_execution_time('30-40,5/3 7,9 * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 7, 5)
        sql(
            "select simples_f_get_next_execution_time('10,44 14 ? 3 2', '2021-03-02 14:10:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 3, 2, 14, 44)
        sql(
            "select simples_f_get_next_execution_time('5/10 6-10 ? 8 6,1','2021-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 8, 2, 6, 5, 0)
    }
}