package pl.petergood.raft

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import pl.petergood.raft.node.*
import kotlin.time.Duration.Companion.seconds

class RaftNodeTest : FunSpec({
    test("nodes should start, elect leader, and stop") {
        val nodeRegistry = SingleMachineNodeRegistry()
        val nodeTransporter = NodeTransporterImpl(nodeRegistry)
        val nodeConfig = NodeConfig()

        runBlocking {
            coroutineScope {
                val nodes: List<Node> = List(3) {
                    val chan: Channel<Message> = Channel()
                    val node = RaftNode(it, nodeConfig, nodeTransporter, this, chan)
                    nodeRegistry.registerNode(node.getId(), SingleMachineChannelingNodeSocket(chan, this))

                    node
                }

                nodes.forEach { it.start() }
                eventually(5.seconds) {
                    nodes.map { it.getStatus() } shouldContainAll listOf(NodeStatus.FOLLOWER, NodeStatus.FOLLOWER, NodeStatus.LEADER)
                }

                nodes.forEach { it.stop() }
                eventually(5.seconds) {
                    nodes.forEach { it.isRunning() shouldBe false }
                }
            }
        }
    }
})