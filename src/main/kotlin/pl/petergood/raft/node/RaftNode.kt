package pl.petergood.raft.node

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import pl.petergood.raft.Message
import pl.petergood.raft.StopNode
import java.util.*

data class NodeState(
    val currentTerm: Int = 0,
    val votedFor: UUID? = null,
    val status: NodeStatus = NodeStatus.STOPPED
)

enum class NodeStatus {
    LEADER, FOLLOWER, CANDIDATE, STOPPED
}

class RaftNode(val id: UUID) : Node {
    private val inputChannel: Channel<Message> = Channel()
    private var state = NodeState()

    override suspend fun start() {
        state = state.copy(status = NodeStatus.CANDIDATE)
        handler()
    }

    override suspend fun stop() {
        inputChannel.send(StopNode())
    }

    override suspend fun dispatchMessage(message: Message) {
        inputChannel.send(message)
    }

    override fun isRunning(): Boolean = state.status != NodeStatus.STOPPED

    private suspend fun handler() {
        while (isRunning()) {
            val message = inputChannel.receive()

            when (message) {
                is StopNode -> {
                    state = state.copy(status = NodeStatus.STOPPED)
                }
            }
        }
    }
}

fun Node.launch(scope: CoroutineScope) {
    scope.launch {
        start()
    }
}