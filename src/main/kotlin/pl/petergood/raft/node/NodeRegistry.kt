package pl.petergood.raft.node

import pl.petergood.raft.RaftMessage
import java.util.*

interface NodeRegistry {
    fun registerNode(nodeId: UUID, nodeSocket: NodeSocket<RaftMessage>)
    fun getAllNodes(): Collection<UUID>

    fun getNodeSocket(nodeId: UUID): NodeSocket<RaftMessage>?
}

class SingleMachineNodeRegistry : NodeRegistry {
    private val nodes: MutableMap<UUID, NodeSocket<RaftMessage>> = mutableMapOf()

    override fun registerNode(nodeId: UUID, nodeSocket: NodeSocket<RaftMessage>) {
        nodes[nodeId] = nodeSocket
    }

    override fun getAllNodes(): Collection<UUID> = nodes.keys

    override fun getNodeSocket(nodeId: UUID): NodeSocket<RaftMessage>? = nodes[nodeId]
}