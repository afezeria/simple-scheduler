package github.afezeria.simplescheduler

import cn.hutool.log.dialect.slf4j.Slf4jLogFactory
import org.intellij.lang.annotations.Language
import java.sql.*

/**
 * @author afezeria
 */
private val logger = Slf4jLogFactory.get()

internal fun Connection.execute(
    @Language("sql") sql: String,
    vararg params: Any?
): MutableList<MutableMap<String, Any?>> {
    return if (params.isNotEmpty()) {
        prepareStatement(sql).use {
            params.forEachIndexed { index, any ->
                it.setObject(index + 1, any)
            }
            try {
                it.execute()
            } finally {
                if (logger.isDebugEnabled) {
                    var warnings = it.warnings
                    while (warnings != null) {
                        logger.debug(warnings.message)
                        warnings = warnings.nextWarning
                    }
                }
            }
            it.resultSet.toList()
        }
    } else {
        createStatement().use {
            try {
                it.execute(sql)
            } finally {
                if (logger.isDebugEnabled) {
                    var warnings = it.warnings
                    while (warnings != null) {
                        logger.debug(warnings.message)
                        warnings = warnings.nextWarning
                    }
                }
            }
            it.resultSet.toList()
        }
    }
}

internal fun ResultSet?.toList(): MutableList<MutableMap<String, Any?>> {
    if (this == null) return mutableListOf()
    val metaData = this.metaData
    val list = mutableListOf<MutableMap<String, Any?>>()
    while (next()) {
        val map = mutableMapOf<String, Any?>()
        (1..metaData.columnCount).forEach {
            val any = getObject(it)
            map[metaData.getColumnName(it)] = when (any) {
                null -> null
                is Time -> any.toLocalTime()
                is Date -> any.toLocalDate()
                is Timestamp -> any.toLocalDateTime()
                else -> any
            }
        }
        list.add(map)
    }
    return list
}