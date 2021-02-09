package github.afezeria.simplescheduler

import org.testcontainers.containers.PostgreSQLContainer

/**
 * @author afezeria
 */
class KPostgreSQLContainer(imageName: String) : PostgreSQLContainer<KPostgreSQLContainer>(imageName)