package github.afezeria.simplescheduler

/**
 * @author afezeria
 */
fun interface ActionProvider {

    fun getTask(actionName: String): ((String) -> Unit)?
}