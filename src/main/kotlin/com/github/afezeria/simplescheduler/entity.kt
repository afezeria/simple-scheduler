package com.github.afezeria.simplescheduler

import java.time.LocalDateTime


/**
 * 计划信息
 * 调度器根据计划生成任务并执行
 * @property id 唯一标识，自动生成
 * @property type 类型，可选值：basic（根据执行间隔计算执行时间） cron（根据表达式计算执行时间）
 * @property cron cron 表达式，cron类型专用
 * @property name 计划名称
 * @property status 计划状态，可选值：active inactive error,计划错误次数或超时次数超过限制值时状态变为error
 * @property executing 是否正在执行
 * @property ord 优先级，0-100，0最高
 * @property intervalTime 执行间隔时间，basic类型专用
 * @property remainingTimes 剩余执行次数，为null时无限执行，指定次数后每次执行减一，
 *      为0时计划状态变为incative，不再执行
 * @property actionName 动作名称，[ActionProvider]接口根据该名称查找实现
 * @property execAfterStart 开始后是否立刻执行，
 * 当 [type] 为 basic ，值为false时第一次执行时间为计划开始时间+执行间隔秒数
 * 当 [type] 为 cron ，值为false时第一次执行时间为计划开始时间后第一个符合表达式的时间
 * @property serialExec 是否并行执行
 * 当 [type] 为 basic 时，值为true时下次执行时间为 [lastExecEndTime]+[intervalTime]s 且不能有任务正在执行，
 *  为false时为 [lastExecStartTime]+[intervalTime]s
 * 当 [type] 为 cron 时，值为true时到达下次执行时间时如果 [executing] 为 true 则跳过这次执行，
 *  为false时到达下次执行时间时直接执行，不判断 [executing] 的值
 * @property totalTimes 总执行次数
 * @property errorTimes 错误次数
 * @property allowErrorTimes 允许错误次数，小于 [errorTimes]+[timeoutTimes] 时计划状态变为 error
 * @property timeout 超时时间，单位：秒
 * @property timeoutTimes 超时次数
 * @property startTime 计划开始时间
 * @property endTime 计划结束时间，可为null
 * @property createTime 创建时间，自动生成
 * @property createUser 创建用户，随意
 * @property planData 计划数据，执行动作时作为参数传给实现
 * @property lastExecStartTime 上一次执行开始时间，自动生成
 * @property lastExecEndTime 上一次执行结束时间，自动生成
 * @property nextExecTime 下一次执行时间，自动生成
 * @property remark 备注
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

/**
 * 调度器信息
 * @property name 名称，不唯一
 * @property startTime 第一次拉取任务的时间
 * @property status 状态，可能的值： active inactive dead
 * @property checkInterval 心跳间隔，单位：秒
 * @property lastHeartbeatTime 最后一次心跳时间，当前时间大于 [lastHeartbeatTime]+3*[checkInterval] 时调度器状态变为 dead
 */
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

/**
 * 任务信息
 * @property planId 所属计划id
 * @property schedulerId 执行该任务的调度器id
 * @property startTime 任务开始时间
 * @property endTime 任务结束时间
 * @property actionName 动作名称
 * @property initData 初始数据，任务创建时从计划的 [PlanInfo.planData] 字段复制
 * @property status 状态，可能的值： start stop error timeout
 * @property timeoutTime 超时时间
 * @property errorMsg 错误信息
 */
class TaskInfo(map: Map<String, Any?>) {
    var id: Int
    var planId: Int
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
        planId = map["plan_id"] as Int
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
