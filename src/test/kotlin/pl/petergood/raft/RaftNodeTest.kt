package pl.petergood.raft

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import pl.petergood.raft.node.*
import java.util.*
import kotlin.time.Duration.Companion.seconds

class RaftNodeTest : FunSpec({
    test("nodes should start and stop") {
        val nodeRegistry = SingleMachineNodeRegistry()
        val nodeTransporter = NodeTransporterImpl(nodeRegistry)
        val nodeConfig = NodeConfig(nodeRegistry = nodeRegistry)

        val nodes: List<Node> = List(1) { RaftNode(UUID.randomUUID(), nodeConfig, nodeTransporter) }

        runBlocking {
            nodes.forEach { it.start() }

            launch {
                delay(500)
                nodes.forEach { it.stop() }

                eventually(5.seconds) {
                    nodes.forEach { it.isRunning() shouldBe false }
                }
            }
        }
    }
})