import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.30"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("cn.hutool:hutool-all:4.5.15")
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.slf4j:slf4j-api:1.7.30")

    testImplementation("com.zaxxer:HikariCP:3.4.5")
    testImplementation("org.postgresql:postgresql:42.2.10")
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.15.1"))
    testImplementation("org.testcontainers:postgresql:1.15.1")

    testImplementation("ch.qos.logback:logback-classic:1.2.3")
    testImplementation("p6spy:p6spy:3.9.1")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation(platform("org.junit:junit-bom:5.7.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.kotest:kotest-assertions-core-jvm:4.3.1")

}
tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "1.8"
    }
}


tasks.withType<Test> {
    useJUnitPlatform()
}

