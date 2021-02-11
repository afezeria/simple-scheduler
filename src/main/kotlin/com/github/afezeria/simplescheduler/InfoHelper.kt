package com.github.afezeria.simplescheduler

import javax.sql.DataSource

/**
 * @author afezeria
 */
open class InfoHelper(private val dataSource: DataSource) {
    /**
     * 查询任务信息
     * @param planId 计划id
     * @param schedulerId 调度器id
     * @param actionName 动作名称，前缀查找
     * @param status 状态
     * @param limit
     * @param offset
     */
    fun getTaskInfo(
        planId: Int? = null,
        schedulerId: Int? = null,
        actionName: String? = null,
        status: String? = null,
        limit: Int = 50,
        offset: Int = 0
    ): List<TaskInfo> {
        if (limit < 1) {
            throw IllegalArgumentException("limit must be greater than 0")
        }
        val conds = mutableListOf<String>()
        val params = mutableListOf<Any>()
        if (planId != null) {
            conds.add("(plan_id = ?)")
            params.add(planId)
        }
        if (schedulerId != null) {
            conds.add("(scheduler_id = ?)")
            params.add(schedulerId)
        }
        if (status != null) {
            conds.add("(status = ?)")
            params.add(status)
        }
        if (actionName != null) {
            conds.add("(action_name like ?)")
            params.add("$actionName%")
        }
        val where = if (conds.isNotEmpty()) {
            conds.joinToString(separator = " and ", prefix = " where ", postfix = " ")
        } else {
            ""
        }
        dataSource.connection.use {
            val execute = it.execute(
                "select * from simples_task $where order by start_time desc limit $limit offset $offset",
                *params.toTypedArray()
            )
            return execute.map { TaskInfo(it) }
        }
    }

    /**
     * 获取调度器信息
     * @param id id
     * @param name 名称
     * @param status 状态
     * @param limit
     * @param offset
     */
    fun getSchedulerInfo(
        id: Int? = null,
        name: String? = null,
        status: String? = null,
        limit: Int = 50,
        offset: Int = 0
    ): List<SchedulerInfo> {
        if (limit < 1) {
            throw IllegalArgumentException("limit must be greater than 0")
        }
        val conds = mutableListOf<String>()
        val params = mutableListOf<Any>()
        if (id != null) {
            conds.add("(id = ?)")
            params.add(id)
        }
        if (name != null) {
            conds.add("(name = ?)")
            params.add(name)
        }
        if (status != null) {
            conds.add("(status = ?)")
            params.add(status)
        }
        val where = if (conds.isNotEmpty()) {
            conds.joinToString(separator = " and ", prefix = " where ", postfix = " ")
        } else {
            ""
        }
        dataSource.connection.use {
            val execute = it.execute(
                "select * from simples_scheduler $where order by start_time desc limit $limit offset $offset",
                *params.toTypedArray()
            )
            return execute.map { SchedulerInfo(it) }
        }
    }

    /**
     * 获取计划信息
     * @param id id
     * @param name 名称
     * @param type 类型
     * @param status 状态
     * @param actionName 动作名称，前缀查找
     * @param limit
     * @param offset
     */
    fun getPlanInfo(
        id: Int? = null,
        name: String? = null,
        type: String? = null,
        status: String? = null,
        actionName: String? = null,
        limit: Int = 50,
        offset: Int = 0
    ): List<PlanInfo> {
        if (limit < 1) {
            throw IllegalArgumentException("limit must be greater than 0")
        }
        val conds = mutableListOf<String>()
        val params = mutableListOf<Any>()
        if (id != null) {
            conds.add("(id = ?)")
            params.add(id)
        }
        if (name != null) {
            conds.add("(name = ?)")
            params.add(name)
        }
        if (type != null) {
            conds.add("(type = ?)")
            params.add(type)
        }
        if (status != null) {
            conds.add("(status = ?)")
            params.add(status)
        }
        if (actionName != null) {
            conds.add("(action_name like ?)")
            params.add("$actionName%")
        }
        val where = if (conds.isNotEmpty()) {
            conds.joinToString(separator = " and ", prefix = " where ", postfix = " ")
        } else {
            ""
        }
        dataSource.connection.use {
            val execute = it.execute(
                "select * from simples_plan $where order by create_time desc limit $limit offset $offset",
                *params.toTypedArray()
            )
            return execute.map { PlanInfo(it) }
        }
    }

}