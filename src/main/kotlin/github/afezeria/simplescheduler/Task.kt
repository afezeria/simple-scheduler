package github.afezeria.simplescheduler

/**
 * @author afezeria
 */
class Task(val id: Int, val initData: String? = null, val body: (String) -> Unit) : Runnable {
    override fun run() {
        body(initData ?: "")
    }
}