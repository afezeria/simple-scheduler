package com.github.afezeria.simplescheduler

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

/**
 * @author afezeria
 */
internal class SchedulerTest : AbstractContainerTest() {

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
        Thread.sleep(2_000)
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
        Thread.sleep(10_000)
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
        Thread.sleep(5_000)
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
        Thread.sleep(2_000)
        list.size shouldBe 1
        scheduler.stop()
    }

    @Test
    fun `multi client test`() {
        val list = mutableListOf<String>()
        val c1 = Scheduler(
            dataSource = dataSource,
            actionProvider = { { list.add("abc") } },
            maximumPoolSize = 10,
            pollInterval = 3,
        )
        val c2 = Scheduler(
            dataSource = dataSource,
            actionProvider = { { list.add("def") } },
            maximumPoolSize = 10,
            pollInterval = 3,
        )
        c1.createBasicPlan(
            name = "test1",
            intervalTime = 2,
            actionName = "abc",
            timeout = 2,
            startTime = LocalDateTime.now(),
            execAfterStart = true,
        )
        c1.start()
        Thread.sleep(2000)
        c2.start()
        Thread.sleep(10_000)
//        执行流程：
//        创建任务，任务立刻执行
//        0s c1 启动并拉取到一条任务，任务下次执行时间为2s
//        1s
//        2s c2 启动并拉取到一条任务，下次执行时间为4s
//        3s c1 拉取任务，返回任务条数0
//        4s
//        5s c2 拉取到一条任务，下次执行时间7s
//        6s c1 拉取任务，返回任务条数0
//        7s
//        8s c2 拉取到一条任务，下次执行时间10s
//        9s c1 拉取任务，返回任务条数0
//       10s
//       11s c2 拉取到一条任务，下次执行时间10s
//       12s 等待结束
        list.size shouldBe 5
        list shouldContainExactly listOf("abc", "def", "def", "def", "def")
        c1.stop()
        c2.stop()

    }
}