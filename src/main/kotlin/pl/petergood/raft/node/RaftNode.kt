package pl.petergood.raft.node

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import pl.petergood.raft.*
import java.util.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class NodeState(
    val currentTerm: Int = 0,
    val votedFor: UUID? = null,
    val status: NodeStatus = NodeStatus.STOPPED,
    val lastLeaderHeartbeat: Instant? = null
)

data class NodeConfig(
    val electionTimeout: Duration = 1.seconds
)

enum class NodeStatus {
    LEADER, FOLLOWER, CANDIDATE, STOPPED
}

class RaftNode(
    private val id: UUID,
    private val config: NodeConfig = NodeConfig(),
    private val nodeRegistry: NodeRegistry
) : Node {
    private val inputChannel: Channel<Message> = Channel()
    private var state = NodeState()

    var timeoutJob: Job? = null

    override suspend fun start(coroutineScope: CoroutineScope) {
        state = state.copy(status = NodeStatus.CANDIDATE)
        timeoutJob = coroutineScope.launch {
            inputChannel.send(CheckTimeout())
            delay(config.electionTimeout)
        }

        handler()
    }

    override suspend fun stop() {
        inputChannel.send(StopNode())
    }

    override suspend fun dispatchMessage(message: Message) {
        inputChannel.send(message)
    }

    override fun isRunning(): Boolean = state.status != NodeStatus.STOPPED
    override fun getId(): UUID = id

    private suspend fun handler() {
        while (isRunning()) {
            val message = inputChannel.receive()

            state = when (message) {
                is StopNode -> {
                    message.handle(state)
                }

                is CheckTimeout -> {
                    message.handle(state, config)
                }

                is ExternalMessage -> {
                    message.handle(state, config)
                }
            }
        }

        handleShutdown()
    }

    private suspend fun handleShutdown() {
        timeoutJob?.cancelAndJoin()
    }

    private suspend fun startElection() {
        state = state.copy(status = NodeStatus.CANDIDATE)
    }
}

fun StopNode.handle(state: NodeState): NodeState = state.copy(status = NodeStatus.STOPPED)

fun CheckTimeout.handle(state: NodeState, config: NodeConfig): NodeState {
    val last: Instant = state.lastLeaderHeartbeat ?: Instant.DISTANT_PAST
    if (Clock.System.now() - last >= config.electionTimeout) {

    }

    return state
}

fun ExternalMessage.handle(state: NodeState, config: NodeConfig): NodeState {
    return when (message) {
        is AppendEntries -> {
            state.copy(lastLeaderHeartbeat = Clock.System.now())
        }

        is RequestVote -> {
            state
        }
    }
}

fun Node.launch(scope: CoroutineScope) {
    scope.launch {
        start(scope)
    }
}