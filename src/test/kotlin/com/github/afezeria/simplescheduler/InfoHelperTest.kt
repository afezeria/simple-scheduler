package com.github.afezeria.simplescheduler

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * @author afezeria
 */
class InfoHelperTest : AbstractContainerTest() {
    @BeforeEach
    fun setUp() {
        dataSource.connection.use {
            it.execute("delete from simples_task")
            it.execute("delete from simples_scheduler")
            it.execute("delete from simples_plan")
        }
    }

    @Test
    fun getTaskInfo() {
        val scheduler = Scheduler(
            dataSource = dataSource,
            actionProvider = { { print("") } },
            maximumPoolSize = 10,
            pollInterval = 1,
        )
        sql(
            """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    timeout_time,status)
                values 
                (1,2,'2020-01-02 00:00:00','print','2020-01-02 00:00:00','start'),
                (1,2,'2020-01-02 00:00:00','print','2020-01-02 00:00:00','start'),
                (2,2,'2020-01-02 00:00:00','print','2020-01-02 00:00:00','error'),
                (2,2,'2020-01-02 00:00:00','print','2020-01-02 00:00:00','error'),
                (3,3,'2020-01-02 00:00:00','print','2020-01-02 00:00:00','error'),
                (3,3,'2020-01-02 00:00:00','print','2020-01-02 00:00:00','error')
                returning *;
            """
        )
        scheduler.getTaskInfo(
            planId = 1,
        ).size shouldBe 2
        scheduler.getTaskInfo(
            schedulerId = 3,
            status = "error",
        ).size shouldBe 2
        scheduler.getTaskInfo(
            actionName = "pr"
        ).size shouldBe 6
        scheduler.getTaskInfo(
            limit = 3,
            offset = 4,
        ).size shouldBe 2
    }

    @Test
    fun getSchedulerInfo() {
        val scheduler = Scheduler(
            dataSource = dataSource,
            actionProvider = { { print("") } },
            maximumPoolSize = 10,
            pollInterval = 1,
        )
        val insRes = sql(
            """
            insert into simples_scheduler (name, start_time, status, check_interval, last_heartbeat_time) 
            values 
            ('a',now(),'active',20,now()),
            ('b',now(),'inactive',20,now()),
            ('c',now(),'dead',20,now())
            returning *;
        """
        )[0]
        scheduler.getSchedulerInfo(insRes["id"] as Int).size shouldBe 1
        scheduler.getSchedulerInfo(name = "a").size shouldBe 1
        scheduler.getSchedulerInfo(status = "dead").size shouldBe 1
        scheduler.getSchedulerInfo(limit = 2).size shouldBe 2
        scheduler.getSchedulerInfo().size shouldBe 3
    }

    @Test
    fun getPlanInfo() {
        val scheduler = Scheduler(
            dataSource = dataSource,
            actionProvider = { { print("") } },
            maximumPoolSize = 10,
            pollInterval = 1,
        )
        val insPlanRes = sql(
            """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time,plan_data,total_times,remaining_times) 
                values 
                ('basic',false,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10), 
                ('basic',false,'bc',20,'send',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10) 
                returning *;
            """
        )[0]
        scheduler.getPlanInfo(insPlanRes["id"] as Int).size shouldBe 1
        scheduler.getPlanInfo(name = "abc").size shouldBe 1
        scheduler.getPlanInfo(type = "cron").size shouldBe 0
        scheduler.getPlanInfo(status = "active")
        scheduler.getPlanInfo(actionName = "s").size shouldBe 1
    }
}