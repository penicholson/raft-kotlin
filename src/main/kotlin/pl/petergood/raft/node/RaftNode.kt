package pl.petergood.raft.node

import kotlinx.coroutines.channels.Channel
import pl.petergood.raft.Message
import java.util.*

class RaftNode(val id: UUID) : Node {
    val inputChannel: Channel<Message> = Channel()

    override fun start() {
    }

    override suspend fun dispatchMessage(message: Message) {
        inputChannel.send(message)
    }
}