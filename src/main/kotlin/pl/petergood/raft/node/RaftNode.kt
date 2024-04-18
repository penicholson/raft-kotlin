package pl.petergood.raft.node

import arrow.core.flatMap
import arrow.core.right
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import pl.petergood.raft.*
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class NodeState(
    val currentTerm: Int = 0,
    val votedFor: UUID? = null,
    val status: NodeStatus = NodeStatus.STOPPED,
    val lastLeaderHeartbeat: Instant? = null
)

data class NodeConfig(
    val electionTimeout: Duration = 1.seconds,
    val nodeRegistry: NodeRegistry
)

enum class NodeStatus {
    LEADER, FOLLOWER, CANDIDATE, STOPPED
}

class RaftNode(
    private val id: UUID,
    private val config: NodeConfig,
    private val nodeTransporter: NodeTransporter
) : Node {
    private val inputChannel: Channel<Message> = Channel()
    var mainJob: Job? = null
    var timeoutJob: Job? = null

    override suspend fun start() {
        timeoutJob = coroutineScope {
            launch {
                inputChannel.send(CheckTimeout())
                delay(config.electionTimeout)
            }
        }

        mainJob = coroutineScope {
            launch {
                handler()
            }
        }
    }

    override suspend fun stop() {
        inputChannel.send(StopNode())
    }

    override suspend fun dispatchMessage(message: Message) {
        inputChannel.send(message)
    }

    override fun isRunning(): Boolean = mainJob?.isActive ?: false
    override fun getId(): UUID = id

    private suspend fun handler() {
        var state = NodeState(status = NodeStatus.FOLLOWER)

        while (state.status != NodeStatus.STOPPED) {
            val message = inputChannel.receive()

            state = when (message) {
                is StopNode -> message.handle(state)
                is CheckTimeout -> message.handle(state)
                is RequestedVotingComplete -> message.handle(state)
                is ExternalMessage -> message.handle(state)
            }
        }

        handleShutdown()
    }

    private suspend fun handleShutdown() {
        timeoutJob?.cancelAndJoin()
    }

    fun StopNode.handle(state: NodeState): NodeState = state.copy(status = NodeStatus.STOPPED)

    suspend fun CheckTimeout.handle(state: NodeState): NodeState {
        val last: Instant = state.lastLeaderHeartbeat ?: Instant.DISTANT_PAST
        if (Clock.System.now() - last >= config.electionTimeout) {
            return startElection(state)
        }

        return state
    }

    fun ExternalMessage.handle(state: NodeState): NodeState {
        return when (message) {
            is AppendEntries -> {
                state.copy(lastLeaderHeartbeat = Clock.System.now())
            }

            is RequestVote -> {
                state
            }
        }
    }

    fun RequestedVotingComplete.handle(state: NodeState): NodeState {
        return state
    }

    private suspend fun startElection(state: NodeState): NodeState {
        val newTerm = state.currentTerm + 1

        coroutineScope {
            launch {
                nodeTransporter.broadcast(id, RequestVote(newTerm, id))
                    .map { it.awaitAll().map { responseMessage ->
                        when (responseMessage) {
                            is RequestVoteResponse -> responseMessage
                        }
                    } }
                    .onRight { dispatchMessage(RequestedVotingComplete(it)) }
                    .onLeft { dispatchMessage(StopNode()) }
            }
        }

        return state.copy(currentTerm = newTerm, status = NodeStatus.CANDIDATE)
    }
}