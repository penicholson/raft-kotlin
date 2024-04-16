package pl.petergood.raft.node

import kotlinx.coroutines.channels.Channel
import pl.petergood.raft.Message
import java.util.UUID

interface NodeRegistry {
    fun registerNode(node: Node)
    fun registerMultipleNodes(nodes: List<Node>)

    suspend fun broadcast(sender: Node, message: Message)
    suspend fun dispatchToDynamicChannel(channelId: UUID, message: Message)
    fun createDynamicChannel(sender: Node): Pair<UUID, Channel<Message>>
}

class SingleMachineNodeRegistry : NodeRegistry {
    val nodes: MutableList<Node> = mutableListOf()
    val dynamicChannels: MutableMap<UUID, Channel<Message>> = mutableMapOf()

    override fun registerNode(node: Node) {
        nodes.add(node)
    }

    override fun registerMultipleNodes(nodes: List<Node>) = nodes.forEach { registerNode(it) }

    override suspend fun broadcast(sender: Node, message: Message) =
        nodes.filter { it.getId() != sender.getId() }
            .forEach { it.dispatchMessage(message) }

    override suspend fun dispatchToDynamicChannel(channelId: UUID, message: Message) {

    }

    override fun createDynamicChannel(sender: Node): Pair<UUID, Channel<Message>> {
        val channel: Channel<Message> = Channel()
        val channelId = UUID.randomUUID()

        dynamicChannels[channelId] = channel

        return Pair(channelId, channel)
    }
}