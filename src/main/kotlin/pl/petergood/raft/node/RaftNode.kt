package pl.petergood.raft.node

import arrow.core.right
import io.github.oshai.kotlinlogging.KotlinLogging
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
    val electionTimeout: Duration = 1.seconds,
)

enum class NodeStatus {
    LEADER, FOLLOWER, CANDIDATE, STOPPED
}

interface Node {
    suspend fun start()
    suspend fun stop()
    suspend fun dispatchMessage(message: Message)

    fun isRunning(): Boolean
    fun getId(): Int
    fun getStatus(): NodeStatus
}

class RaftNode(
    private val id: Int,
    private val config: NodeConfig,
    private val nodeTransporter: NodeTransporter,
    private val coroutineScope: CoroutineScope,
    private val inputChannel: Channel<Message> = Channel()
) : Node {
    // main coroutine executing this node's runtime loop
    var mainJob: Job? = null

    // job periodically checking if heartbeats are being received from leader
    var timeoutJob: Job? = null

    // represents state of Node
    private var state = NodeState(status = NodeStatus.FOLLOWER)

    private val logger = KotlinLogging.logger {}

    override suspend fun start() {
        timeoutJob = coroutineScope.launch {
            while (true) {
                inputChannel.send(CheckTimeout)
                delay(config.electionTimeout)
            }
        }

        mainJob = coroutineScope.launch {
            handler()
        }
    }

    override suspend fun stop() {
        inputChannel.send(StopNode)
    }

    override suspend fun dispatchMessage(message: Message) {
        inputChannel.send(message)
    }

    override fun isRunning(): Boolean = state.status != NodeStatus.STOPPED
    override fun getId(): Int = id
    override fun getStatus(): NodeStatus = state.status

    // main node handler function
    private suspend fun handler() {
        while (isRunning()) {
            val message = inputChannel.receive()
            logger.debug { "Node $id received message $message" }

            state = when (message) {
                is StopNode ->
                    message.handle(state)

                is CheckTimeout ->
                    message.handle(state)

                is RequestedVotingComplete ->
                    message.handle(state)

                is ExternalMessage ->
                    message.handle(state)
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
        if (Clock.System.now() - last >= config.electionTimeout && getStatus() != NodeStatus.CANDIDATE) {
            return startElection(state)
        }

        return state
    }

    // handling for Raft messages
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
        logger.debug { "Starting election on $id" }
        val newTerm = state.currentTerm + 1

        coroutineScope.launch {
            coroutineScope {
                nodeTransporter.broadcast(id, RequestVote(newTerm, id))
                    .map {
                        it.awaitAll().map { responseMessage ->
                            when (responseMessage) {
                                is RequestVoteResponse -> {
                                    logger.debug { "Got resp" }
                                    responseMessage
                                }
                            }
                        }
                    }

                logger.debug { "OUT" }
            }
        }

        return state.copy(currentTerm = newTerm, status = NodeStatus.CANDIDATE)
    }
}