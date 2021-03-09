package script

import java.io.FileWriter
import java.nio.file.Path

/**
 * @author afezeria
 */
fun main() {
    val sqlScript =
        Thread.currentThread().contextClassLoader.getResourceAsStream("table-function-trigger.sql")!!
            .readAllBytes().decodeToString()
    val regex = Regex("create table (\\w+)|create (?:or replace )?function (\\w+)")
    val newRegex = regex.findAll(sqlScript).map {
        it.groupValues.run { get(1).takeIf { it.isNotEmpty() } ?: get(2) }
    }.joinToString("|", prefix = "(?<!\\w)(", postfix = ")(?!\\w)")
    val regex1 = Regex(newRegex)
    val schema = "simple_schema"
    FileWriter(
        Path.of("sql/a.sql").toFile()
    ).use {
        it.write("create schema \"$schema\";\n")
    }
    FileWriter(
        Path.of("sql/table-function-trigger.sql").toFile()
    ).use {
        it.write(sqlScript.replace(regex1) { "\"$schema\".${it.value}" })
    }
}