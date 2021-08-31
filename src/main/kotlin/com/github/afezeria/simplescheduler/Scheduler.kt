package com.github.afezeria.simplescheduler

import org.slf4j.LoggerFactory
import java.io.PrintWriter
import java.io.StringWriter
import java.net.InetAddress
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import javax.sql.DataSource


/**
 * @author afezeria
 * @param dataSource 数据源
 * @param actionProvider 提供任务实现的接口，
 * @param maximumPoolSize 线程池最大线程数/该调度器同时执行的任务数的最大值
 * @param pollInterval 轮询间隔，单位：秒，查询任务的时间间隔
 * @param batchSize 每次获取任务的数量的最大值，默认等于maximumPoolSize
 * @param ordAsc 是否按优先级正序查询任务，为false时优先获取优先级低的任务，默认为true
 * @param planNamePrefix 计划名称前缀，只获取指定前缀的任务，默认为null
 * @param printStackTraceToErrorMsg 保存错误信息时是否保存异常堆栈，默认为false
 * @param taskCallback 用户自定义回调接口，在任务开始前和任务开始后执行
 * @param schema 数据库schema
 * @param name 调度器名称，只是方便人查看的名称，可重复，默认为 进程pid@主机名@随机uuid
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
    private val taskCallback: TaskCallback = object : TaskCallback {},
    schema: String? = null,
    name: String? = null,
) : InfoHelper(dataSource) {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private lateinit var thread: Thread
    private val pool: ThreadPoolExecutor
    private val schema = schema?.let { "\"$schema\"." } ?: ""
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
            name ?: "${
                ProcessHandle.current().pid()
            }@${InetAddress.getLocalHost().hostName}@${
                UUID.randomUUID().toString().replace("-", "")
            }"

    }

    /**
     * 开始获取和执行任务
     */
    fun start() {
        thread = CoreThread()
        thread.start()
    }

    /**
     * 停止获取和执行任务
     */
    fun stop() {
        pool.shutdown()
        thread.interrupt()
    }

    /**
     * 创建普通计划
     * @param name 计划名称
     * @param intervalTime 执行间隔，单位：秒
     * @param actionName 动作名称
     * @param timeout 超时时间，单位：秒
     * @param startTime 计划开始时间
     * @param endTime 计划结束时间
     * @param ord 优先级，从0-100，0为最高优先级
     * @param remainingTimes 剩余执行次数
     * @param execAfterStart 开始后是否立刻执行，为false时第一次执行时间为计划开始时间+执行间隔秒数
     * @param allowErrorTimes 允许错误次数，超过指定次数后将停止执行计划
     * @param serialExec 并行执行，为true时执行间隔从上一次执行时间开始计算，为false时从上一次执行结束时间开始计算
     * @param planData 计划数据，执行动作时将会作为参数传给动作函数，无要求
     * @param createUser 创建人，无要求
     */
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
        remark: String? = null,
    ): PlanInfo {
        dataSource.connection.use {
            val res = it.execute(
                """
                insert into ${schema}simples_plan (type, name, ord, interval_time, remaining_times, action_name, 
                    exec_after_start, serial_exec, allow_error_times, timeout, start_time, end_time, 
                    create_user, plan_data,remark) 
                values ('basic',?,?,?,?,?,?,?,?,?,?,?,?,?,?) returning *;
            """, name, ord, intervalTime, remainingTimes, actionName,
                execAfterStart, serialExec, allowErrorTimes, timeout, startTime, endTime,
                createUser, planData, remark
            )
            return PlanInfo(res[0])
        }
    }

    /**
     * 更新计划
     */
    fun updatePlan(plan: PlanInfo): PlanInfo {
        dataSource.connection.use {
            val res = it.execute(
                """
                update ${schema}simples_plan 
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

    /**
     * 创建cron表达式计划
     * @param name 计划名称
     * @param cron cron 表达式
     * @param actionName 动作名称
     * @param timeout 超时时间，单位：秒
     * @param startTime 计划开始时间
     * @param endTime 计划结束时间
     * @param ord 优先级，从0-100，0为最高优先级
     * @param remainingTimes 剩余执行次数
     * @param execAfterStart 开始后是否立刻执行，为false时第一次执行时间为开始时间后第一个符合cron表达式的时间
     * @param allowErrorTimes 允许错误次数，超过指定次数后将停止执行计划
     * @param serialExec 并行执行，为true时到达符合表达式的时间时如果有任务正在执行则不执行
     * @param planData 计划数据，执行动作时将会作为参数传给动作函数，无要求
     * @param createUser 创建人，无要求
     */
    fun createCronPlan(
        name: String,
        cron: String,
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
        remark: String? = null,
    ): PlanInfo {
        dataSource.connection.use {
            val res = it.execute(
                """
                insert into ${schema}simples_plan (type, name, ord, cron, remaining_times, action_name, 
                    exec_after_start, serial_exec, allow_error_times, timeout, start_time, end_time, 
                    create_user, plan_data,remark) 
                values ('cron',?,?,?,?,?,?,?,?,?,?,?,?,?,?) returning *;
            """, name, ord, cron, remainingTimes, actionName,
                execAfterStart, serialExec, allowErrorTimes, timeout, startTime, endTime,
                createUser, planData, remark
            )
            return PlanInfo(res[0])
        }
    }


    inner class Task(
        val id: Int,
        val actionName: String,
        val initData: String? = null,
        val body: (String) -> Unit
    ) :
        Runnable {
        override fun run() {
            logger.info("task start. [id:{}]", id)
            var ex: Exception? = null
            try {
                taskCallback.before(id, actionName, initData)
                body(initData ?: "")
            } catch (e: Exception) {
                ex = e
                val errMsg = if (printStackTraceToErrorMsg) {
                    val writer = StringWriter()
                    e.printStackTrace(PrintWriter(writer))
                    writer.toString()
                } else {
                    e.message
                }
                logger.warn("task failed. [id:{}]", id, e)
                dataSource.connection.use {
                    it.execute("select ${schema}simples_f_mark_task_error(?,?)", id, errMsg)
                }
            } finally {
                taskCallback.after(id, actionName, initData, ex)
            }
            if (ex == null) {
                dataSource.connection.use {
                    it.execute("select * from ${schema}simples_f_mark_task_completed(?)", id)
                }
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
                        "select * from ${schema}simples_f_get_task(?,?,?,?,?,?)",
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
                                    update ${schema}simples_task 
                                    set status = 'error',
                                        error_msg = 'action not find'
                                    where id = ?
                                """, taskId
                                )
                            } else {
                                val task = Task(taskId, actionName, initData, body)
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
                val list = it.execute("select * from ${schema}simples_f_mark_timeout_task()")
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
                val list = it.execute("select * from ${schema}simples_f_mark_dead_scheduler()")
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
