package com.github.afezeria.simplescheduler

import com.p6spy.engine.spy.P6DataSource
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.intellij.lang.annotations.Language
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import java.io.FileWriter
import java.nio.file.Path
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
            val sqlScript =
                Thread.currentThread().contextClassLoader.getResourceAsStream("table-function-trigger.sql")!!
                    .readAllBytes().decodeToString()
            val regex = Regex("create table (\\w+)|create (?:or replace )?function (\\w+)")
            val newRegex = regex.findAll(sqlScript).map {
                it.groupValues.run { get(1).takeIf { it.isNotEmpty() } ?: get(2) }
            }.joinToString("|", prefix = "(?<!\\w)(", postfix = ")(?!\\w)")
            val regex1 = Regex(newRegex)
            val schema = "simple_schema"
            FileWriter(
                Path.of("sql/a.sql").toFile()
            ).use {
                it.write("create schema \"$schema\";\n")
            }
            FileWriter(
                Path.of("sql/table-function-trigger.sql").toFile()
            ).use {
                it.write(sqlScript.replace(regex1) { "\"$schema\".${it.value}" })
            }

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
                    this.schema = schema
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