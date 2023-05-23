plugins {
    kotlin("jvm") version "1.8.20"
}

group = "griffio"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.hsqldb:hsqldb:2.7.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}
