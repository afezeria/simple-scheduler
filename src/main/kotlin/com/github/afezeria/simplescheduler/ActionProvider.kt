package com.github.afezeria.simplescheduler

/**
 * 提供执行的任务的具体实现
 * @author afezeria
 */
fun interface ActionProvider {

    /**
     * 根据动作名称返回动作执行函数
     * 返回值为null时scheduler将打印错误信息并将任务标记为error
     */
    fun getTask(actionName: String): ((String) -> Unit)?
}