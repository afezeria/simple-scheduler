package com.github.afezeria.simplescheduler.sql

import com.github.afezeria.simplescheduler.AbstractContainerTest
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
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 9,20,3 * ?','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 20, 5, 0)

        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 ? * 2L,5L','2021-02-28 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 3, 26, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 ? * 2L,5','2021-02-28 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 3, 5, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 20,L * ?','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 20, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 L * ?','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 28, 5, 0)

//        W选项
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 LW * ?','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 28, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 6W,14W 3 ?','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 3, 5, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 14W * ?','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 15, 5, 0)

//        #选项
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 ? * 5#3','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 19, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 ? * 2#4,6#2','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 13, 5, 0)
        sql(
            "select simples_f_get_next_execution_time('* 5-10,7/4 ? * 5#4,6L','2021-02-12 11:12:03') as a"
        )[0]["a"] shouldBe LocalDateTime.of(2021, 2, 26, 5, 0)
    }
}