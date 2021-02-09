package github.afezeria.simplescheduler

/**
 * @author afezeria
 */
fun waitSeconds(int: Int) {
    Thread.sleep(int.toLong() * 1000)
}
