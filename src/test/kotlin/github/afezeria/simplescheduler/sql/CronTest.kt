package github.afezeria.simplescheduler.sql

import github.afezeria.simplescheduler.AbstractContainerTest
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

/**
 * @author afezeria
 */
class CronTest : AbstractContainerTest() {

    @Test
    fun `cron expr test`() {
        sql(
            "select simples_f_get_next_execution_time('* * * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 0, 1)
        sql(
            "select simples_f_get_next_execution_time('10-20 * * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 0, 10)
        sql(
            "select simples_f_get_next_execution_time('* 5,6,7 * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* * 2/3 * ?', '2020-01-03 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 5, 0, 0)
        sql(
            "select simples_f_get_next_execution_time('* * ? * 2', '2021-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 1, 5, 0, 0)
        sql(
            "select simples_f_get_next_execution_time('30-40,5/3 7,9 * * *', '2020-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2020, 1, 1, 7, 5)
        sql(
            "select simples_f_get_next_execution_time('10,44 14 ? 3 2', '2021-03-02 14:10:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 3, 2, 14, 44)
        sql(
            "select simples_f_get_next_execution_time('5/10 6-10 ? 8 6,1','2021-01-01 00:00:00') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 8, 2, 6, 5, 0)
    }
}