package github.afezeria.simplescheduler

import com.p6spy.engine.spy.P6DataSource
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.intellij.lang.annotations.Language
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import java.time.Duration
import javax.sql.DataSource

/**
 * @author afezeria
 */
abstract class AbstractContainerTest {
    companion object {
        val container: PostgreSQLContainer<Nothing>
        val dataSource: DataSource

        init {
            container = PostgreSQLContainer<Nothing>("postgres:12.5-alpine").apply {
                withFileSystemBind(
                    "sql", "/docker-entrypoint-initdb.d",
                    BindMode.READ_ONLY
                )
                withExposedPorts(5432)
                waitingFor(
                    HostPortWaitStrategy()
                        .withStartupTimeout(Duration.ofSeconds(10))
                )
            }
            container.start()
            dataSource = P6DataSource(
                HikariDataSource(HikariConfig().apply {
                    jdbcUrl = container.jdbcUrl
                    username = container.username
                    password = container.password
                })
            )
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
}