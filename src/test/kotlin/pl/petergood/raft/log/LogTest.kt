package pl.petergood.raft.log

import arrow.core.none
import arrow.core.right
import arrow.core.some
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.equals.shouldBeEqual

class LogTest : DescribeSpec({
    describe("findFirstIndex") {
        describe("when log contains matching entry") {
            it("should return index") {
                val log = InMemoryLog()
                log.appendEntry(0, 1)
                log.appendEntry(1, 1)

                log.findFirstIndex(1, 1) shouldBeEqual 1.some()
            }
        }

        describe("when log does not contain matching entry") {
            describe("when no index found") {
                it("should return empty Option") {
                    val log = InMemoryLog()
                    log.appendEntry(0, 1)
                    log.appendEntry(1, 1)

                    log.findFirstIndex(2, 1) shouldBeEqual none()
                }
            }

            describe("when terms at index mismatch") {
                it("should return empty Option") {
                    val log = InMemoryLog()
                    log.appendEntry(0, 1)
                    log.appendEntry(1, 1)

                    log.findFirstIndex(1, 2) shouldBeEqual none()
                }
            }
        }
    }

    describe("truncateFromIndex") {
        it("should truncate log starting at index exclusive") {
            val log = InMemoryLog()
            List(10) { it }.forEach { log.appendEntry(it, 0) }

            log.truncateFromIndex(5)

            log.getSize() shouldBeEqual 6
            log.getEntryAtIndex(5) shouldBeEqual LogEntry(5, 0, false, 5).right()
        }
    }
})