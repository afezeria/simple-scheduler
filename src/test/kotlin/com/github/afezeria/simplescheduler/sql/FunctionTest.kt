package com.github.afezeria.simplescheduler.sql

import com.github.afezeria.simplescheduler.AbstractContainerTest
import com.github.afezeria.simplescheduler.PlanInfo
import com.github.afezeria.simplescheduler.TaskInfo
import com.github.afezeria.simplescheduler.execute
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime

/**
 * @author afezeria
 */
class FunctionTest : AbstractContainerTest() {

    @BeforeEach
    fun setUp() {
        dataSource.connection.use {
            it.execute("delete from simples_task")
            it.execute("delete from simples_scheduler")
            it.execute("delete from simples_plan")
        }
    }

    @Nested
    inner class MarkTimeoutTask {
        @Test
        fun `mark a task`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time) 
                values ('basic',true,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00') 
                returning *;
            """
            )[0]
            sql(
                """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    status, timeout_time) 
                values (${insPlanRes["id"]},2,'2020-01-01 00:00:00','print','start',('2020-01-01 00:00:00'::timestamptz+20*interval '1 second'));
            """
            )
            sql("select * from simples_f_mark_timeout_task()")
            val res = sql("select * from simples_task")[0]
            val info = TaskInfo(res)
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            info.status shouldBe "timeout"
            plan.timeoutTimes shouldBe 1
            plan.executing shouldBe false
        }

        @Test
        fun `the timeout task is not the last one`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time) 
                values ('basic',true,'abc',20,'print',20,'2020-01-01 00:00:00','2021-01-01 00:00:00','2020-01-01 00:00:00') 
                returning *;
            """
            )[0]
//            task.start_time != plan.last_start_time and current_timestamp > task.timeout_time
            sql(
                """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    status, timeout_time) 
                values (${insPlanRes["id"]},2,'2020-01-02 00:00:00','print','start',('2020-01-02 00:00:00'::timestamptz+20*interval '1 second'));
            """
            )
            sql("select * from simples_f_mark_timeout_task()")
            val res = sql("select * from simples_task")[0]
            val info = TaskInfo(res)
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            info.status shouldBe "timeout"
            plan.timeoutTimes shouldBe 1
            plan.executing shouldBe true
        }

    }

    @Nested
    inner class MarkErrorTask {

        @Test
        fun `mark a task`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time) 
                values ('basic',true,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00') 
                returning *;
            """
            )[0]
            val insTaskRes = sql(
                """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    status, timeout_time) 
                values (${insPlanRes["id"]},2,'2020-01-01 00:00:00','print','start',('2020-01-01 00:00:00'::timestamptz+20*interval '1 second'))
                returning *;
            """
            )[0]
            sql("select simples_f_mark_task_error(?,?)", insTaskRes["id"], "abc")
            val res = sql("select * from simples_task")[0]
            val info = TaskInfo(res)
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            info.status shouldBe "error"
            info.errorMsg shouldBe "abc"
            info.endTime shouldNotBe null
            Duration.between(info.endTime, LocalDateTime.now()).seconds shouldBeLessThan 10L
            plan.errorTimes shouldBe 1
            plan.executing shouldBe false
            plan.lastExecEndTime shouldBe info.endTime
        }

        @Test
        fun `the error task is not the last one`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time) 
                values ('basic',true,'abc',20,'print',20,'2020-01-01 00:00:00','2021-01-01 00:00:00','2020-01-01 00:00:00') 
                returning *;
            """
            )[0]
            val insTaskRes = sql(
                """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    status, timeout_time) 
                values (${insPlanRes["id"]},2,'2020-01-02 00:00:00','print','start',('2020-01-02 00:00:00'::timestamptz+20*interval '1 second'))
                returning *;
            """
            )[0]
            sql("select simples_f_mark_task_error(?,?)", insTaskRes["id"], "abc")
            val res = sql("select * from simples_task")[0]
            val info = TaskInfo(res)
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            info.status shouldBe "error"
            info.errorMsg shouldBe "abc"
            info.endTime shouldNotBe null
            Duration.between(info.endTime, LocalDateTime.now()).seconds shouldBeLessThan 10L
            plan.errorTimes shouldBe 1
            plan.executing shouldBe true
            plan.lastExecEndTime shouldNotBe info.endTime
        }

        @Test
        fun `error task has timeout`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time) 
                values ('basic',true,'abc',20,'print',20,'2020-01-01 00:00:00','2021-01-01 00:00:00','2020-01-01 00:00:00') 
                returning *;
            """
            )[0]
            val insTaskRes = sql(
                """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    status, timeout_time) 
                values (${insPlanRes["id"]},2,'2020-01-01 00:00:00','print','timeout',('2020-01-01 00:00:00'::timestamptz+20*interval '1 second'))
                returning *;
            """
            )[0]
            sql("select simples_f_mark_task_error(?,?)", insTaskRes["id"], "abc")
            val res = sql("select * from simples_task")[0]
            val info = TaskInfo(res)
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            info.status shouldBe "error"
            info.errorMsg shouldBe "abc"
            info.endTime shouldNotBe null
            Duration.between(info.endTime, LocalDateTime.now()).seconds shouldBeLessThan 10L
            plan.errorTimes shouldBe 1
            plan.executing shouldBe true
            plan.lastExecEndTime shouldNotBe info.endTime
        }
    }

    @Nested
    inner class MarkCompletedTask {
        @Test
        fun `mark a task`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time) 
                values ('basic',true,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00') 
                returning *;
            """
            )[0]
            val insTaskRes = sql(
                """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    status, timeout_time) 
                values (${insPlanRes["id"]},2,'2020-01-01 00:00:00','print','start',('2020-01-01 00:00:00'::timestamptz+20*interval '1 second'))
                returning *;
            """
            )[0]
            sql("select simples_f_mark_task_completed(?)", insTaskRes["id"])
            val res = sql("select * from simples_task")[0]
            val info = TaskInfo(res)
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            info.status shouldBe "stop"
            info.endTime shouldNotBe null
            Duration.between(info.endTime, LocalDateTime.now()).seconds shouldBeLessThan 10L
            plan.executing shouldBe false
            plan.lastExecEndTime shouldBe info.endTime
        }

        @Test
        fun `the completed task has timeout`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time) 
                values ('basic',true,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00') 
                returning *;
            """
            )[0]
            val insTaskRes = sql(
                """
                insert into simples_task (plan_id, scheduler_id, start_time, action_name, 
                    status, timeout_time) 
                values (${insPlanRes["id"]},2,'2020-01-01 00:00:00','print','timeout',('2020-01-01 00:00:00'::timestamptz+20*interval '1 second'))
                returning *;
            """
            )[0]
            sql("select simples_f_mark_task_completed(?)", insTaskRes["id"])
            val res = sql("select * from simples_task")[0]
            val info = TaskInfo(res)
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            info.status shouldBe insTaskRes["status"]
            info.endTime shouldNotBe null
            Duration.between(info.endTime, LocalDateTime.now()).seconds shouldBeLessThan 10L
            plan.executing shouldBe true
            plan.lastExecEndTime shouldNotBe info.endTime
        }
    }

    @Test
    fun markDeadScheduler() {
        sql(
            """
            insert into simples_scheduler (name, start_time, status, check_interval, last_heartbeat_time) 
            values 
            ('abc1','2020-01-01 00:00:00','active',200,?),
            ('abc2','2020-01-01 00:00:00','active',200,?),
            ('abc3','2020-01-01 00:00:00','active',200,?)
            ;
        """,
            LocalDateTime.now(),
            LocalDateTime.now().minusSeconds(601),
            LocalDateTime.now().minusSeconds(500)
        )
        val res = sql("select * from simples_f_mark_dead_scheduler()")
        res.size shouldBe 1
        res[0]["s_name"] shouldBe "abc2"
        val schedulerRes = sql(
            "select * from simples_scheduler ss where name = ?",
            "abc2"
        )[0]
        schedulerRes["status"] shouldBe "dead"
    }

    @Nested
    inner class GetTask {
        @Test
        fun `update current scheduler last_heartbeat_time`() {
            sql(
                """
                insert into simples_scheduler (id,name, start_time, status, check_interval, last_heartbeat_time) 
                values (1,'abc','2020-01-01 00:00:00','active',20,'2020-01-01 00:00:00');
            """
            )
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                1, "bcd", 20, 20, null, true
            )[0]
            res["s_id"] shouldBe 1
            res["action_name"] shouldBe null
            res["task_id"] shouldBe null
            res["init_data"] shouldBe null
            val schedulerRes = sql("select * from simples_scheduler ss")
            schedulerRes.size shouldBe 1
            (schedulerRes[0]["last_heartbeat_time"] as LocalDateTime).toLocalDate() shouldBe LocalDate.now()
        }

        @Test
        fun `new scheduler get a task`() {
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 20, null, true
            )[0]
            res["s_id"] shouldNotBe 52420430
            res["action_name"] shouldBe null
            res["task_id"] shouldBe null
            res["init_data"] shouldBe null
        }

        @Test
        fun `new scheduler get task`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time,plan_data,total_times,remaining_times) 
                values ('basic',false,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10) 
                returning *;
            """
            )[0]
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 20, null, true
            )[0]
            res["s_id"] shouldNotBe 52420430
            res["action_name"] shouldBe "print"
            res["task_id"] shouldNotBe null
            res["init_data"] shouldBe "dd"
            val taskRes = sql("select * from simples_task")[0]
            val planRes = sql("select * from simples_plan sp")[0]
            val plan = PlanInfo(planRes)
            val info = TaskInfo(taskRes)
            plan.apply {
                lastExecStartTime shouldBe info.startTime
                lastExecEndTime shouldBe plan.lastExecStartTime
                executing shouldBe true
                totalTimes shouldBe 11
                remainingTimes shouldBe 9
            }
            info.apply {
                plan.id shouldBe planId
                schedulerId shouldBe res["s_id"]
                id shouldBe res["task_id"]
                actionName shouldBe plan.actionName
                initData shouldBe plan.planData
                status shouldBe "start"
                timeoutTime shouldBe startTime.plusSeconds(20)
            }
        }

        @Test
        fun `get multiple task`() {
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
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 20, null, true
            )
            res.size shouldBe 2
        }

        @Test
        fun `filter task by name prefix`() {
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
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 20, "a", true
            )
            res.size shouldBe 1
            res[0]["action_name"] shouldBe "print"
        }

        @Test
        fun `default get task order by ord asc`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (ord,type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time,plan_data,total_times,remaining_times) 
                values 
                (40,'basic',false,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10), 
                (50,'basic',false,'bc',20,'send',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10) 
                returning *;
            """
            )[0]
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 1, null, true
            )
            res.size shouldBe 1
            res[0]["action_name"] shouldBe "print"
        }

        @Test
        fun `get task order by ord desc`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (ord,type, executing,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time,plan_data,total_times,remaining_times) 
                values 
                (40,'basic',false,'abc',20,'print',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10), 
                (50,'basic',false,'bc',20,'send',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10) 
                returning *;
            """
            )[0]
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 1, null, false
            )
            res.size shouldBe 1
            res[0]["action_name"] shouldBe "send"
        }

        @Test
        fun `update plan status`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, status,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time,plan_data,total_times,remaining_times) 
                values 
                ('basic','inactive','a',20,'print1',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10),
                ('basic','inactive','b',20,'print2',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,null),
                -- 错误的任务
                ('basic','error' ,'c',20,'send',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10),
                -- 未到开始时间的任务
                ('basic','inactive' ,'d',20,'send',20,'5000-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,10),
                -- 剩余执行次数为0的任务
                ('basic','inactive' ,'e',20,'send',20,'2020-01-01 00:00:00','2020-01-01 00:00:00','2020-01-01 00:00:00',
                'dd',10,0) 
                returning *;
            """
            )[0]
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 20, null, true
            )
            res.size shouldBe 2
            res.map { it["action_name"] } shouldContainExactlyInAnyOrder listOf("print1", "print2")
        }

        @Test
        fun `get parallel tasks`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,serial_exec,name, interval_time, action_name, timeout, 
                    start_time,last_exec_start_time,last_exec_end_time,plan_data,total_times,remaining_times) 
                values 
                -- 到达执行时间
                ('basic',true,false,'abc',2,'print1',20,'2020-01-01 00:00:00',?,?,'dd',10,10),
                -- 未到达执行时间
                ('basic',true,false,'def',2,'print2',20,'2020-01-01 00:00:00',?,?,'dd',10,10) 
                returning *;
            """,
                LocalDateTime.now().minusSeconds(2),
                LocalDateTime.now().minusSeconds(2),
                LocalDateTime.now(),
                LocalDateTime.now(),
            )[0]
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 20, null, true
            )[0]
            res["action_name"] shouldBe "print1"
            val taskRes = sql("select * from simples_task")[0]
            val planRes = sql("select * from simples_plan sp where name = ?", "abc")[0]
            val plan = PlanInfo(planRes)
            val info = TaskInfo(taskRes)
            plan.apply {
                lastExecStartTime shouldBe info.startTime
                lastExecEndTime shouldBe plan.lastExecStartTime
                executing shouldBe true
                totalTimes shouldBe 11
                remainingTimes shouldBe 9
            }
            info.apply {
                plan.id shouldBe planId
                schedulerId shouldBe res["s_id"]
                id shouldBe res["task_id"]
                actionName shouldBe plan.actionName
                initData shouldBe plan.planData
                status shouldBe "start"
            }
        }

        @Test
        fun `get serial tasks`() {
            val insPlanRes = sql(
                """
                insert into simples_plan (type, executing,serial_exec,name, interval_time, action_name, 
                    timeout, start_time,plan_data,total_times,remaining_times) 
                values 
                -- 当前有任务正在执行
                ('basic',true,true,'b',10,'print1',20,'2020-01-01 00:00:00','dd',10,10),
                -- 当前没有任务在执行但是以最后结束时间计算还没到下次执行时间
                ('basic',false,true,'c',10,'print2',20,'2020-01-01 00:00:00','dd',10,10),
                -- 没有任务在执行且已经到了执行时间
                ('basic',false,true,'d',10,'print3',20,'2020-01-01 00:00:00','dd',10,10)
                returning *;
            """
            )
            val now = LocalDateTime.now()
            sql(
                "update simples_plan set last_exec_start_time = ? , last_exec_end_time = ? where name = ?",
                now, now, "b"
            )
            sql(
                "update simples_plan set last_exec_start_time = ? , last_exec_end_time = ? where name = ?",
                now, now, "c"
            )
            sql(
                "update simples_plan set last_exec_start_time = ? , last_exec_end_time = ? where name = ?",
                now.minusSeconds(20), now.minusSeconds(15), "d"
            )
            val res = sql(
                "select * from simples_f_get_task(?,?,?,?,?,?)",
                52420430, "abc", 20, 20, null, true
            )[0]
            res["action_name"] shouldBe "print3"
            val taskRes = sql("select * from simples_task")[0]
            val planRes = sql("select * from simples_plan where name = ?", "d")[0]
            val plan = PlanInfo(planRes)
            val info = TaskInfo(taskRes)
            plan.apply {
                lastExecStartTime shouldBe info.startTime
                lastExecEndTime shouldBe plan.lastExecStartTime
                executing shouldBe true
                totalTimes shouldBe 11
                remainingTimes shouldBe 9
            }
            info.apply {
                plan.id shouldBe planId
                schedulerId shouldBe res["s_id"]
                id shouldBe res["task_id"]
                actionName shouldBe plan.actionName
                initData shouldBe plan.planData
                status shouldBe "start"
            }
        }
    }
}