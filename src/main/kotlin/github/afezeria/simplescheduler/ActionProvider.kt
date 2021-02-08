package github.afezeria.simplescheduler

/**
 * @author afezeria
 */
interface ActionProvider {

    fun getTask(actionName: String): ((String) -> Unit)?
}