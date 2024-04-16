package pl.petergood.raft.node

import pl.petergood.raft.Message

interface NodeRegistry {
    fun registerNode(node: Node)
    fun registerMultipleNodes(nodes: List<Node>)

    suspend fun broadcast(sender: Node, message: Message)
}

class SingleMachineNodeRegistry : NodeRegistry {
    val nodes: MutableList<Node> = mutableListOf()

    override fun registerNode(node: Node) {
        nodes.add(node)
    }

    override fun registerMultipleNodes(nodes: List<Node>) = nodes.forEach { registerNode(it) }

    override suspend fun broadcast(sender: Node, message: Message) =
        nodes.filter { it.getId() != sender.getId() }
            .forEach { it.dispatchMessage(message) }
}