package pl.petergood.raft

import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import pl.petergood.raft.node.Node
import pl.petergood.raft.node.RaftNode
import java.util.*

class RaftNodeTest : FunSpec({
    test("nodes should start") {
        val nodes: List<Node> = List(4) { RaftNode(UUID.randomUUID()) }

        runBlocking {
            nodes.forEach { it.start() }

            launch {
                delay(1000)
                nodes.forEach { it.stop() }
            }
        }
    }
})