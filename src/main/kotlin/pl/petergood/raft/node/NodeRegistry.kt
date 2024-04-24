package pl.petergood.raft.node

import pl.petergood.raft.RaftMessage
import java.util.*

interface NodeRegistry {
    fun registerNode(nodeId: Int, nodeSocket: NodeSocket<RaftMessage>)
    fun getAllNodes(): Collection<Int>
    fun getNumberOfNodes(): Int

    fun getNodeSocket(nodeId: Int): NodeSocket<RaftMessage>?
}

class SingleMachineNodeRegistry : NodeRegistry {
    private val nodes: MutableMap<Int, NodeSocket<RaftMessage>> = mutableMapOf()

    override fun registerNode(nodeId: Int, nodeSocket: NodeSocket<RaftMessage>) {
        nodes[nodeId] = nodeSocket
    }

    override fun getAllNodes(): Collection<Int> = nodes.keys
    override fun getNumberOfNodes(): Int = nodes.size

    override fun getNodeSocket(nodeId: Int): NodeSocket<RaftMessage>? = nodes[nodeId]
}