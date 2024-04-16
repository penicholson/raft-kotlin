package pl.petergood.raft

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.common.ExperimentalKotest
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import pl.petergood.raft.node.Node
import pl.petergood.raft.node.RaftNode
import pl.petergood.raft.node.SingleMachineNodeRegistry
import pl.petergood.raft.node.launch
import java.util.*
import kotlin.time.Duration.Companion.seconds

class RaftNodeTest : FunSpec({
    test("nodes should start and stop") {
        val nodeRegistry = SingleMachineNodeRegistry()
        val nodes: List<Node> = List(5) { RaftNode(UUID.randomUUID(), nodeRegistry = nodeRegistry) }
        nodeRegistry.registerMultipleNodes(nodes)

        runBlocking {
            nodes.forEach { it.launch(this) }

            launch {
                delay(200)
                nodes.forEach { it.stop() }

                eventually(5.seconds) {
                    nodes.forEach { it.isRunning() shouldBe false }
                }
            }
        }
    }
})