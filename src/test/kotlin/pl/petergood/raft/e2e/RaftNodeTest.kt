package pl.petergood.raft.e2e

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import pl.petergood.raft.log.InMemoryLog
import pl.petergood.raft.log.LogEntry
import pl.petergood.raft.node.*
import kotlin.time.Duration.Companion.seconds

class RaftNodeTest : DescribeSpec({
    suspend fun createCluster(nodeConfig: NodeConfig,
                              nodeTransporter: NodeTransporter,
                              nodeRegistry: NodeRegistry): List<Node> {
        val nodes: List<Node> = List(3) { RaftNode(it, nodeConfig, nodeTransporter, nodeRegistry, InMemoryLog()) }

        nodes.forEach { it.start() }
        eventually(10.seconds) {
            nodes.map { it.getStatus() } shouldContainAll listOf(
                NodeStatus.FOLLOWER,
                NodeStatus.FOLLOWER,
                NodeStatus.LEADER
            )
        }

        return nodes
    }

    describe("election smoke test") {
        it("nodes should start, elect leader, and stop") {
            val nodeRegistry = SingleMachineNodeRegistry()
            val nodeTransporter = NodeTransporterImpl(nodeRegistry)
            val nodeConfig = NodeConfig()

            runBlocking {
                val nodes = createCluster(nodeConfig, nodeTransporter, nodeRegistry)

                nodes.forEach { it.stop() }
                eventually(10.seconds) {
                    nodes.forEach { it.isRunning() shouldBe false }
                }
            }
        }
    }

    describe("storage smoke test") {
        it("should store value on leader and replicate to followers") {
            val nodeRegistry = SingleMachineNodeRegistry()
            val nodeTransporter = NodeTransporterImpl(nodeRegistry)
            val nodeConfig = NodeConfig()
            val log = InMemoryLog()

            runBlocking {
                val nodes = createCluster(nodeConfig, nodeTransporter, nodeRegistry)
                val leader = nodes.findLast { it.getStatus() == NodeStatus.LEADER }
                leader.shouldNotBeNull()

                leader.store("Hello World!")

                eventually(5.seconds) {
                    leader.getLog().getEntries() shouldContainAll listOf(LogEntry(0, false, "Hello World!"))
                }
            }
        }
    }
})