package github.afezeria.simplescheduler

import com.p6spy.engine.spy.P6DataSource
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.kotest.matchers.shouldBe
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
internal class SchedulerTest {
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

    lateinit var scheduler: Scheduler

    @BeforeEach
    fun setUp() {
        dataSource.connection.use {
            it.execute("delete from simples_task")
            it.execute("delete from simples_scheduler")
            it.execute("delete from simples_plan")
        }
    }

    @AfterEach
    fun tearDown() {
        if (this::scheduler.isInitialized) {
            scheduler.stop()
        }
        println("end")
    }

    @Test
    fun start() {
        scheduler = Scheduler(
            dataSource = dataSource,
            actionProvider = { _ -> { println("Perform a task") } },
            maximumPoolSize = 10,
            pollInterval = 20,
        )
        scheduler.start()
        waitSeconds(2)
        val res = sql("select * from simples_scheduler")
        res.size shouldBe 1
        res[0].apply {
            get("id") shouldBe scheduler.id
            get("name") shouldBe scheduler.name
        }
    }

    @Test
    fun createBasicPlan() {
        val list = mutableListOf<String>()
        scheduler = Scheduler(
            dataSource = dataSource,
            actionProvider = { { list.add("abc") } },
            maximumPoolSize = 10,
            pollInterval = 1,
        )
        scheduler.start()
        scheduler.createBasicPlan(
            name = "test1",
            intervalTime = 2,
            actionName = "abc",
            timeout = 10,
            startTime = LocalDateTime.now(),
            remainingTimes = 3,
            execAfterStart = true,
        )
        waitSeconds(10)
        list.size shouldBe 3
        scheduler.stop()
    }

    @Test
    fun `task timeout test`() {

        val list = mutableListOf<String>()
        scheduler = Scheduler(
            dataSource = dataSource,
            actionProvider = { { Thread.sleep(2000);list.add("abc") } },
            maximumPoolSize = 10,
            pollInterval = 1,
        )
        scheduler.start()
        val plan = scheduler.createBasicPlan(
            name = "test1",
            intervalTime = 2,
            actionName = "abc",
            timeout = 1,
            startTime = LocalDateTime.now(),
            remainingTimes = 1,
        )
        waitSeconds(5)
        val planInfo = sql("select * from simples_plan where id = ?", plan.id).run {
            size shouldBe 1
            PlanInfo(get(0))
        }
        planInfo.timeoutTimes shouldBe 1

        val res = sql("select * from simples_task where plain_id = ?", plan.id)
        res.size shouldBe 1
        val info = TaskInfo(res[0])
        info.status shouldBe "timeout"
    }

    @Test
    fun createCronPlan() {
        val list = mutableListOf<String>()
        scheduler = Scheduler(
            dataSource = dataSource,
            actionProvider = { { list.add("abc") } },
            maximumPoolSize = 10,
            pollInterval = 1,
        )
        scheduler.start()
        scheduler.createCronPlan(
            cron = "* * * * *",
            name = "test",
            actionName = "",
            timeout = 0,
            startTime = LocalDateTime.now().minusMinutes(1),
            remainingTimes = null,
            serialExec = false,
        )
        waitSeconds(2)
        list.size shouldBe 1
        scheduler.stop()
    }
}