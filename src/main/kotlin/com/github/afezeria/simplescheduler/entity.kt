package com.github.afezeria.simplescheduler

import java.time.LocalDateTime


/**
 * @author afezeria
 */
class PlanInfo(map: Map<String, Any?>) {
    var id: Int
    var type: String
    var cron: String?
    var name: String
    var status: String
    var executing: Boolean
    var ord: Int
    var intervalTime: Int?
    var remainingTimes: Int?
    var actionName: String
    var execAfterStart: Boolean
    var serialExec: Boolean
    var totalTimes: Int
    var errorTimes: Int
    var allowErrorTimes: Int?
    var timeout: Int
    var timeoutTimes: Int
    var startTime: LocalDateTime
    var endTime: LocalDateTime?
    var createTime: LocalDateTime
    var createUser: String?
    var planData: String?
    var lastExecStartTime: LocalDateTime
    var lastExecEndTime: LocalDateTime
    var nextExecTime: LocalDateTime
    var remark: String?

    init {
        id = map["id"] as Int
        type = map["type"] as String
        cron = map["cron"] as String?
        name = map["name"] as String
        status = map["status"] as String
        executing = map["executing"] as Boolean
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
        remark = map["remark"] as String?
    }

}

class SchedulerInfo(map: Map<String, Any?>) {
    var id: Int
    var name: String
    var startTime: LocalDateTime
    var status: String
    var checkInterval: Int
    var lastHeartbeatTime: LocalDateTime

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
    var id: Int
    var plainId: Int
    var schedulerId: Int
    var startTime: LocalDateTime
    var endTime: LocalDateTime?
    var actionName: String
    var initData: String?
    var status: String
    var timeoutTime: LocalDateTime
    var errorMsg: String?

    init {
        id = map["id"] as Int
        plainId = map["plain_id"] as Int
        schedulerId = map["scheduler_id"] as Int
        startTime = map["start_time"] as LocalDateTime
        endTime = map["end_time"] as LocalDateTime?
        actionName = map["action_name"] as String
        initData = map["init_data"] as String?
        status = map["status"] as String
        timeoutTime = map["timeout_time"] as LocalDateTime
        errorMsg = map["error_msg"] as String?
    }


}
