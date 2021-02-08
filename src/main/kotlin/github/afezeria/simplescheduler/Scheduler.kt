package github.afezeria.simplescheduler

import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
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
    private val batchSize: Int = maximumPoolSize,
    private val pollInterval: Int,
    private val ordAsc: Boolean = true,
    private val planNamePrefix: String? = null,
    name: String? = null,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)
    lateinit var thread: Thread
    private val pool: ThreadPoolExecutor
    val name: String
    private var sid: Int? = null

    init {
        if (batchSize > maximumPoolSize) {
            throw IllegalArgumentException("batchSize must less than maxPoolSize")
        }
        pool = ThreadPoolExecutor(
            1, maximumPoolSize, 1L,
            TimeUnit.MINUTES,
            SynchronousQueue(), ThreadPoolExecutor.AbortPolicy()
        )
        this.name = name ?: ManagementFactory.getRuntimeMXBean().name
    }


    fun start() {
        thread = CoreThread()
        thread.start()

    }

    fun stop() {
        pool.shutdown()
        try {
            thread.interrupt()
        } catch (e: InterruptedException) {
            logger.info("stop pull task")
        }

    }

    inner class CoreThread : Thread() {

        override fun run() {
            while (true) {
                execTask()
                markScheduler()
                markTask()
                sleep((pollInterval * 1000).toLong())
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
                        sid, name, pollInterval, number, planNamePrefix, ordAsc
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
                            val initData = data["init_data"] as String
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
                        "task timeout. [plan_id:{},plan_name:{},task_id:{},task_start_time:{},scheduler_id:{},scheduler_name:{}]",
                        m["plan_id"],
                        m["plan_name"],
                        m["task_id"],
                        m["task_start_time"],
                        m["scheduler_id"],
                        m["scheduler_name"]
                    )
                }
            }
        }

        fun markScheduler() {
            dataSource.connection.use {
                val list = it.execute("select * from simples_mark_dead_scheduler()")
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
