plugins {
    kotlin("jvm") version "1.9.23"
}

group = "org.example"
version = "1.0-SNAPSHOT"

val koTestVersion = "5.8.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1-Beta")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.5.0")

    implementation("io.arrow-kt:arrow-core:1.2.1")
    implementation("io.arrow-kt:arrow-fx-coroutines:1.2.1")

    testImplementation("io.kotest:kotest-runner-junit5:$koTestVersion")
    testImplementation("io.kotest:kotest-assertions-core:$koTestVersion")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}