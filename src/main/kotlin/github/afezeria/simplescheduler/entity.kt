package github.afezeria.simplescheduler

import java.time.LocalDateTime


/**
 * @author afezeria
 */
class PlanInfo(map: Map<String, Any?>) {
    val id: Int
    val type: String
    val cron: String?
    val name: String
    val status: String
    val executing: Boolean
    val schedulerId: Int?
    val ord: Int
    val intervalTime: Int?
    val remainingTimes: Int?
    val actionName: String
    val execAfterStart: Boolean
    val serialExec: Boolean
    val totalTimes: Int
    val errorTimes: Int
    val allowErrorTimes: Int?
    val timeout: Int
    val timeoutTimes: Int
    val startTime: LocalDateTime
    val endTime: LocalDateTime?
    val createTime: LocalDateTime
    val createUser: String?
    val planData: String?
    val lastExecStartTime: LocalDateTime
    val lastExecEndTime: LocalDateTime
    val nextExecTime: LocalDateTime

    init {
        id = map["id"] as Int
        type = map["type"] as String
        cron = map["cron"] as String?
        name = map["name"] as String
        status = map["status"] as String
        executing = map["executing"] as Boolean
        schedulerId = map["scheduler_id"] as Int?
        ord = map["ord"] as Int
        intervalTime = map["interval_time"] as Int?
        remainingTimes = map["remaining_times"] as Int?
        actionName = map["action_name"] as String
        execAfterStart = map["exec_after_start"] as Boolean
        serialExec = map["serial_exec"] as Boolean
        totalTimes = map["total_times"] as Int
        errorTimes = map["error_times"] as Int
        allowErrorTimes = map["allow_error_times"] as Int?
        timeout = map["timeout"] as Int
        timeoutTimes = map["timeout_times"] as Int
        startTime = map["start_time"] as LocalDateTime
        endTime = map["end_time"] as LocalDateTime?
        createTime = map["create_time"] as LocalDateTime
        createUser = map["create_user"] as String?
        planData = map["plan_data"] as String?
        lastExecStartTime = map["last_exec_start_time"] as LocalDateTime
        lastExecEndTime = map["last_exec_end_time"] as LocalDateTime
        nextExecTime = map["next_exec_time"] as LocalDateTime
    }

}

class SchedulerInfo(map: Map<String, Any?>) {
    val id: Int
    val name: String
    val startTime: LocalDateTime
    val status: String
    val checkInterval: Int
    val lastHeartbeatTime: LocalDateTime

    init {
        id = map["id"] as Int
        name = map["name"] as String
        startTime = map["start_time"] as LocalDateTime
        status = map["status"] as String
        checkInterval = map["check_interval"] as Int
        lastHeartbeatTime = map["last_heartbeat_time"] as LocalDateTime
    }
}

class TaskInfo(map: Map<String, Any?>) {
    val id: Int
    val plainId: Int
    val schedulerId: Int
    val startTime: LocalDateTime
    val endTime: LocalDateTime?
    val actionName: String
    val initData: String?
    val status: String
    val timeout: Int
    val errorMsg: String?

    init {
        id = map["id"] as Int
        plainId = map["plain_id"] as Int
        schedulerId = map["scheduler_id"] as Int
        startTime = map["start_time"] as LocalDateTime
        endTime = map["end_time"] as LocalDateTime?
        actionName = map["action_name"] as String
        initData = map["init_data"] as String?
        status = map["status"] as String
        timeout = map["timeout"] as Int
        errorMsg = map["error_msg"] as String?
    }


}
