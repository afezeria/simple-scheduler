package com.github.afezeria.simplescheduler

import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.sql.*

/**
 * @author afezeria
 */
private val logger = LoggerFactory.getLogger("com.github.afezeria.simplescheduler.JdbcExtension")

internal fun Connection.execute(
    @Language("sql") sql: String,
    vararg params: Any?
): MutableList<MutableMap<String, Any?>> {
    if (logger.isDebugEnabled) {
        logger.debug(
            """
            sql: {}
            parameters: {}
        """, sql, params
        )
    }
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
            val list = it.resultSet.toList()
            if (logger.isDebugEnabled) {
                logger.debug("result: {}", list)
            }
            list
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
            val list = it.resultSet.toList()
            if (logger.isDebugEnabled) {
                logger.debug("result: {}", list)
            }
            list
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