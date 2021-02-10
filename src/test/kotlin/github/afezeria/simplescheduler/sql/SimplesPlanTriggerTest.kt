package github.afezeria.simplescheduler.sql

import com.p6spy.engine.spy.P6DataSource
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import github.afezeria.simplescheduler.KPostgreSQLContainer
import github.afezeria.simplescheduler.execute
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import java.time.Duration
import java.time.LocalDateTime
import javax.sql.DataSource

/**
 * @author afezeria
 */
class SimplesPlanTriggerTest {
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
                    jdbcUrl =
                        "jdbc:postgresql://localhost:5432/test"
                    username = "test"
                    password = "123456"
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

    @BeforeEach
    fun setUp() {
        dataSource.connection.use {
            it.execute("delete from simples_task")
            it.execute("delete from simples_scheduler")
            it.execute("delete from simples_plan")
        }
    }


    @Nested
    inner class Insert {
        @Test
        fun basic() {
            sql(
                """
                    insert into simples_plan (type, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time) 
                    values ('basic','a',60,'print',true,true,10,'2020-01-01 00:00:00')
                    returning *;
            """
            )[0].apply {
                get("next_exec_time") shouldBe LocalDateTime.of(2020, 1, 1, 0, 0)
                get("last_exec_start_time") shouldBe get("start_time")
                get("last_exec_end_time") shouldBe get("start_time")
            }
            sql(
                """
                    insert into simples_plan (type, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time) 
                    values ('basic','b',60,'print',false,true,10,'2020-01-01 00:00:00')
                    returning *;
            """
            )[0].apply {
                get("next_exec_time") shouldBe LocalDateTime.of(2020, 1, 1, 0, 0, 0).plusSeconds(60)
            }
        }

        @Test
        fun cron() {
            sql(
                """
                insert into simples_plan (type, cron, name,action_name, exec_after_start, timeout, start_time) 
                values ('cron','* * * * *','abc','print',true,20,'2020-01-01 00:00:00')
                returning *;
            """
            )[0].apply {
                get("next_exec_time") shouldBe LocalDateTime.of(2020, 1, 1, 0, 0)
            }
            sql(
                """
                insert into simples_plan (type, cron, name,action_name, exec_after_start, timeout, start_time) 
                values ('cron','* * * * *','abcd','print',false,20,'2020-01-01 00:00:00')
                returning *;
            """
            )[0].apply {
                get("next_exec_time") shouldBe LocalDateTime.of(2020, 1, 1, 0, 1)
            }
        }
    }

    @Nested
    inner class Update {
        @Test
        fun `type cron, start_time change and exec_after_start is false`() {
            val init = sql(
                """
                insert into simples_plan (type, cron, name,action_name, exec_after_start, timeout, start_time) 
                values ('cron','* * * * *','abc','print',false,20,'2020-01-01 00:00:00')
                returning *;
            """
            )[0]
            val change =
                sql("update simples_plan sp set start_time = '2021-01-01 00:00:00' returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2021, 1, 1, 0, 1)
        }

        @Test
        fun `type cron, start_time change and exec_after_start is true`() {
            val init = sql(
                """
                insert into simples_plan (type, cron, name,action_name, exec_after_start, timeout, start_time) 
                values ('cron','* * * * *','abc','print',true,20,'2020-01-01 00:00:00')
                returning *;
            """
            )[0]
            val change =
                sql("update simples_plan sp set start_time = '2021-01-01 00:00:00' returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2021, 1, 1, 0, 0)
        }

        @Test
        fun `type cron, last_exec_start_time change`() {
            val init = sql(
                """
                insert into simples_plan (type, cron, name,action_name, exec_after_start, timeout, start_time) 
                values ('cron','* * * * *','abc','print',true,20,'2020-01-01 00:00:00')
                returning *;
            """
            )[0]
            val change =
                sql("update simples_plan sp set last_exec_start_time = '2021-01-01 00:00:00' returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2021, 1, 1, 0, 1)
        }

        @Test
        fun `type cron, cron change`() {
            val init = sql(
                """
                insert into simples_plan (type, cron, name,action_name, exec_after_start, timeout, start_time) 
                values ('cron','* * * * *','abc','print',true,20,'2020-01-01 00:00:00')
                returning *;
            """
            )[0]
            val change =
                sql("update simples_plan sp set cron = '* 1 * * *' returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2020, 1, 1, 1, 0)
        }

        @Test
        fun `type basic, serial_exec is true and last_exec_end_time change`() {
            val init = sql(
                """
                    insert into simples_plan (type, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time) 
                    values ('basic','b',60,'print',true,true,10,'2020-01-01 00:00:00')
                    returning *;
            """
            )[0]
            val change =
                sql("update simples_plan sp set last_exec_end_time = '2021-01-01 00:00:00' returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2021, 1, 1, 0, 1)
        }

        @Test
        fun `type basic, serial_exec is false and last_exec_start_time change`() {
            val init = sql(
                """
                    insert into simples_plan (type, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time) 
                    values ('basic','b',60,'print',true,false,10,'2020-01-01 00:00:00')
                    returning *;
            """
            )[0]
            val change =
                sql("update simples_plan sp set last_exec_start_time = '2021-01-01 00:00:00' returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2021, 1, 1, 0, 1)
        }

        @Test
        fun `type basic, serial_exec change`() {
            val init = sql(
                """
                    insert into simples_plan (type, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time) 
                    values ('basic','b',60,'print',true,true,10,'2020-01-01 00:00:00')
                    returning *;
            """
            )[0]

            var change =
                sql("update simples_plan sp set last_exec_start_time = '2020-01-01 00:00:00',last_exec_end_time = '2020-02-01 00:00:00' returning *")[0]
            change["next_exec_time"] shouldNotBe LocalDateTime.of(2020, 1, 1, 0, 1)
            change =
                sql("update simples_plan sp set serial_exec = false returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2020, 1, 1, 0, 1)
            change =
                sql("update simples_plan sp set serial_exec = true returning *")[0]
            change["next_exec_time"] shouldBe LocalDateTime.of(2020, 2, 1, 0, 1)
        }

        @Test
        fun `status change to error when timeout_times plus error_times greater than allow_error_times`() {
            val init = sql(
                """
                    insert into simples_plan (type, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time,allow_error_times) 
                    values ('basic','b',60,'print',true,true,10,'2020-01-01 00:00:00',10)
                    returning *;
            """
            )[0]

            val change =
                sql("update simples_plan sp set timeout_times=5,error_times=6 returning *")[0]
            change["status"] shouldBe "error"
        }

        @Test
        fun `status change to inactive when remaining_times becomes 0`() {
            val init = sql(
                """
                    insert into simples_plan (type, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time,remaining_times) 
                    values ('basic','b',60,'print',true,true,10,'2020-01-01 00:00:00',1)
                    returning *;
            """
            )[0]

            val change =
                sql("update simples_plan sp set remaining_times=0 returning *")[0]
            change["status"] shouldBe "inactive"
        }

        @Test
        fun `set error count to 0, when status change from error to active`() {
            val init = sql(
                """
                    insert into simples_plan (type,status, name, interval_time, action_name, exec_after_start, 
                        serial_exec, timeout, start_time,timeout_times,error_times) 
                    values ('basic','error','b',60,'print',true,true,10,'2020-01-01 00:00:00',1,1)
                    returning *;
            """
            )[0]

            val change =
                sql("update simples_plan sp set status='active' returning *")[0]
            change["timeout_times"] shouldBe 0
            change["error_times"] shouldBe 0
        }
    }

}