package com.github.afezeria.simplescheduler

/**
 * @author afezeria
 */
interface TaskCallback {
    /**
     * 任务开始前执行
     */
    fun before(id: Int, actionName: String, initData: String?) {
    }

    /**
     * 任务开始后执行
     */
    fun after(id: Int, actionName: String, initData: String?, ex: Exception?) {
    }
}