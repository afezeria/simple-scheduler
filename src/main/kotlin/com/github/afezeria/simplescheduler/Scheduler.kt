package com.github.afezeria.simplescheduler

import org.slf4j.LoggerFactory
import java.io.PrintWriter
import java.io.StringWriter
import java.net.InetAddress
import java.time.LocalDateTime
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import javax.sql.DataSource


/**
 * @author afezeria
 */
class Scheduler(
    private val dataSource: DataSource,
    private val actionProvider: ActionProvider,
    private val maximumPoolSize: Int,
    private val pollInterval: Int,
    private val batchSize: Int = maximumPoolSize,
    private val ordAsc: Boolean = true,
    private val planNamePrefix: String? = null,
    private val printStackTraceToErrorMsg: Boolean = false,
    name: String? = null,
) : InfoHelper(dataSource) {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private lateinit var thread: Thread
    private val pool: ThreadPoolExecutor
    val name: String
    private var sid: Int? = null
    val id: Int?
        get() = sid

    init {
        if (batchSize > maximumPoolSize) {
            throw IllegalArgumentException("batchSize must less than maxPoolSize")
        }
        if (maximumPoolSize < 1) {
            throw IllegalArgumentException("maximumPoolSize must greater than 0")
        }
        pool = ThreadPoolExecutor(
            1, maximumPoolSize, 1L,
            TimeUnit.MINUTES,
            SynchronousQueue(), ThreadPoolExecutor.AbortPolicy()
        )
        this.name =
            name ?: "${ProcessHandle.current().pid()}@${InetAddress.getLocalHost().hostName}"

    }


    fun start() {
        thread = CoreThread()
        thread.start()
    }

    fun stop() {
        pool.shutdown()
        thread.interrupt()
    }

    fun createBasicPlan(
        name: String,
        intervalTime: Int,
        actionName: String,
        timeout: Int,
        startTime: LocalDateTime,
        endTime: LocalDateTime? = null,
        ord: Int = 50,
        remainingTimes: Int? = null,
        execAfterStart: Boolean = false,
        allowErrorTimes: Int? = null,
        serialExec: Boolean = false,
        planData: String? = null,
        createUser: String? = null,
    ): PlanInfo {
        dataSource.connection.use {
            val res = it.execute(
                """
                insert into simples_plan (type, name, ord, interval_time, remaining_times, action_name, 
                    exec_after_start, serial_exec, allow_error_times, timeout, start_time, end_time, 
                    create_user, plan_data) 
                values ('basic',?,?,?,?,?,?,?,?,?,?,?,?,?) returning *;
            """, name, ord, intervalTime, remainingTimes, actionName,
                execAfterStart, serialExec, allowErrorTimes, timeout, startTime, endTime,
                createUser, planData
            )
            return PlanInfo(res[0])
        }
    }

    fun updatePlan(plan: PlanInfo): PlanInfo {
        dataSource.connection.use {
            val res = it.execute(
                """
                update simples_plan 
                set type = ?,
                    cron = ?,
                    name = ?,
                    status = ?,
                    executing = ?,
                    ord = ?,
                    interval_time = ?,
                    remaining_times = ?,
                    action_name = ?,
                    exec_after_start = ?,
                    serial_exec = ?,
                    total_times = ?,
                    error_times = ?,
                    allow_error_times = ?,
                    timeout = ?,
                    timeout_times = ?,
                    start_time = ?,
                    end_time = ?,
                    create_time = ?,
                    create_user = ?,
                    plan_data = ?,
                    last_exec_start_time = ?,
                    last_exec_end_time = ?,
                    next_exec_time = ?,
                    remark = ?
                where id = ?
                returning *;
            """,
                plan.id,
                plan.type,
                plan.cron,
                plan.name,
                plan.status,
                plan.executing,
                plan.ord,
                plan.intervalTime,
                plan.remainingTimes,
                plan.actionName,
                plan.execAfterStart,
                plan.serialExec,
                plan.totalTimes,
                plan.errorTimes,
                plan.allowErrorTimes,
                plan.timeout,
                plan.timeoutTimes,
                plan.startTime,
                plan.endTime,
                plan.createTime,
                plan.createUser,
                plan.planData,
                plan.lastExecStartTime,
                plan.lastExecEndTime,
                plan.nextExecTime,
                plan.remark,
            )[0]
            return PlanInfo(res)
        }
    }

    fun createCronPlan(
        cron: String,
        name: String,
        actionName: String,
        timeout: Int,
        startTime: LocalDateTime,
        endTime: LocalDateTime? = null,
        ord: Int = 50,
        remainingTimes: Int? = null,
        allowErrorTimes: Int? = null,
        execAfterStart: Boolean = false,
        serialExec: Boolean = false,
        planData: String? = null,
        createUser: String? = null,
    ): PlanInfo {
        dataSource.connection.use {
            val res = it.execute(
                """
                insert into simples_plan (type, name, ord, cron, remaining_times, action_name, 
                    exec_after_start, serial_exec, allow_error_times, timeout, start_time, end_time, 
                    create_user, plan_data) 
                values ('cron',?,?,?,?,?,?,?,?,?,?,?,?,?) returning *;
            """, name, ord, cron, remainingTimes, actionName,
                execAfterStart, serialExec, allowErrorTimes, timeout, startTime, endTime,
                createUser, planData
            )
            return PlanInfo(res[0])
        }
    }


    inner class Task(val id: Int, val initData: String? = null, val body: (String) -> Unit) :
        Runnable {
        override fun run() {
            logger.info("task start. [id:{}]", id)
            try {
                body(initData ?: "")
            } catch (e: Exception) {
                val errMsg = if (printStackTraceToErrorMsg) {
                    val writer = StringWriter()
                    e.printStackTrace(PrintWriter(writer))
                    writer.toString()
                } else {
                    e.message
                }
                logger.info("task failed. [id:{}]", id, e)
                dataSource.connection.use {
                    it.execute("select simples_f_mark_task_error(?,?)", id, errMsg)
                }
                return
            }
            dataSource.connection.use {
                it.execute("select * from simples_f_mark_task_completed(?)", id)
            }

        }
    }

    inner class CoreThread : Thread() {

        override fun run() {
            while (true) {
                execTask()
                markScheduler()
                markTask()
                try {
                    sleep((pollInterval * 1000).toLong())
                } catch (e: InterruptedException) {
                    break
                }
            }
        }

        fun execTask() {
            logger.info("pull task")
            dataSource.connection.use {
                var number = maximumPoolSize - pool.activeCount
                if (number != 0) {
                    if (number > batchSize) number = batchSize


                    val execute = it.execute(
                        "select * from simples_f_get_task(?,?,?,?,?,?)",
                        sid, this@Scheduler.name, pollInterval, number, planNamePrefix, ordAsc
                    )
                    logger.info(
                        "pulled {} tasks",
                        when {
                            execute.size > 1 -> execute.size
                            execute[0]["task_id"] != null -> 1
                            else -> 0
                        }
                    )
                    for (data in execute) {
                        sid = data["s_id"] as Int
                        data["task_id"]?.run {
                            val taskId = this as Int
                            val initData = data["init_data"] as String?
                            val actionName = data["action_name"] as String
                            val body = actionProvider.getTask(actionName)
                            if (body == null) {
                                logger.warn(
                                    "submit task failure, cannot find action:{}, task id:{}",
                                    actionName,
                                    taskId
                                )
                                it.execute(
                                    """
                                    update simples_task 
                                    set status = 'error',
                                        error_msg = 'action not find'
                                    where id = ?
                                """, taskId
                                )
                            } else {
                                val task = Task(taskId, initData, body)
                                pool.execute(task)
                            }
                        }
                    }
                } else {
                    logger.info("active thread has reached the maximumPoolSize, no pull task")
                }
            }

        }

        fun markTask() {
            dataSource.connection.use {
                val list = it.execute("select * from simples_f_mark_timeout_task()")
                for (m in list) {
                    logger.warn(
                        "task timeout. [plan_id:{},plan_name:{},action_name:{},scheduler_id:{},task_id:{},task_start_time:{}]",
                        m["plan_id"],
                        m["plan_name"],
                        m["action_name"],
                        m["scheduler_id"],
                        m["task_id"],
                        m["task_start_time"],
                    )
                }
            }
        }

        fun markScheduler() {
            dataSource.connection.use {
                val list = it.execute("select * from simples_f_mark_dead_scheduler()")
                for (m in list) {
                    logger.warn(
                        "scheduler did not end properly. [scheduler_id:{},scheduler_name]",
                        m["s_id"],
                        m["s_name"]
                    )
                }
            }
        }
    }
}
