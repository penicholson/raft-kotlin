package pl.petergood.raft.node

import arrow.core.right
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import pl.petergood.raft.*
import java.util.*
import kotlin.math.log
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit

data class NodeState(
    val currentTerm: Int = 0,
    val votedFor: UUID? = null,
    val status: NodeStatus = NodeStatus.STOPPED,
    val lastLeaderHeartbeat: Instant? = null,

    // The term in which this node last voted
    val voteCastInTerm: Int = -1
)

data class NodeConfig(
    val electionTimeout: Duration = 1.seconds,
    val electionTimeoutVariance: Duration = 1000.milliseconds,
    val leaderHeartbeatInterval: Duration = 500.milliseconds
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
    private val nodeRegistry: NodeRegistry,
    private val coroutineScope: CoroutineScope,
    private val inputChannel: Channel<Message> = Channel()
) : Node {
    // main coroutine executing this node's runtime loop
    var mainJob: Job? = null

    // job periodically checking if heartbeats are being received from leader
    var timeoutJob: Job? = null

    // Job that periodically dispatches heartbeat message to followers
    // This job is only active when the current node is a leader
    var leaderHeartbeatJob: Job? = null

    // represents state of Node
    private var state = NodeState(status = NodeStatus.FOLLOWER)

    private val logger = KotlinLogging.logger {}

    override suspend fun start() {
        timeoutJob = coroutineScope.launch {
            try {
                while (true) {
                    // select random delay variance from interval [0, electionTimeoutVariance]
                    val nextVariance = Random.nextInt(config.electionTimeoutVariance.toInt(DurationUnit.MILLISECONDS))
                    delay(config.electionTimeout + nextVariance.milliseconds)

                    inputChannel.send(CheckTimeout)
                }
            } catch (e: CancellationException) {
                logger.debug { e }
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

        logger.debug { "Starting shutdown" }
        handleShutdown()
    }

    private suspend fun handleShutdown() {
        timeoutJob?.cancelAndJoin()
        leaderHeartbeatJob?.cancelAndJoin()
        logger.debug { "Shutdown complete" }
    }

    fun StopNode.handle(state: NodeState): NodeState = state.copy(status = NodeStatus.STOPPED)

    suspend fun CheckTimeout.handle(state: NodeState): NodeState {
        val last: Instant = state.lastLeaderHeartbeat ?: Instant.DISTANT_PAST

        // We trigger a new election if no heartbeat has been received from the leader within the timeout
        if (Clock.System.now() - last >= config.electionTimeout && state.status != NodeStatus.LEADER) {
            return startElection(state)
        }

        return state
    }

    // handling for Raft messages
    suspend fun ExternalMessage.handle(state: NodeState): NodeState {
        return when (message) {
            // new entries
            is AppendEntries -> {
                if (message.term > state.currentTerm) {
                    if (state.status != NodeStatus.FOLLOWER) {
                        logger.debug { "Node $id transitioning from ${state.status} to FOLLOWER" }
                    }

                    state.copy(lastLeaderHeartbeat = Clock.System.now(), currentTerm = message.term, status = NodeStatus.FOLLOWER)
                } else {
                    state.copy(lastLeaderHeartbeat = Clock.System.now())
                }
            }

            // vote request from another node
            is RequestVote -> {
                if (message.term <= state.currentTerm || state.voteCastInTerm >= message.term) {
                    // got voting request from outdated node
                    responseSocket.dispatch(RequestVoteResponse(state.currentTerm, false))
                    return state
                }

                responseSocket.dispatch(RequestVoteResponse(state.currentTerm, true))
                state.copy(voteCastInTerm = state.currentTerm)
            }
        }
    }

    // invoked when a vote initiated by this node has been completed (all other nodes have voted)
    fun RequestedVotingComplete.handle(state: NodeState): NodeState {
        val votesPerTerm = responses.groupBy { it.term }
        // Under typical circumstances this should be currentTerm - 1
        val prevTerm = votesPerTerm.keys.maxOf { it }
        val won = votesPerTerm[prevTerm]
            ?.count { it.voteGranted }

            // +1 is the current node
            ?.let { it + 1 > nodeRegistry.getNumberOfNodes() / 2} ?: false

        // Transition to leader state or go back to follower
        return if (won) transitionToLeader(state) else state.copy(status = NodeStatus.FOLLOWER)
    }

    private suspend fun startElection(state: NodeState): NodeState {
        logger.debug { "Starting election on $id" }
        val newTerm = state.currentTerm + 1

        coroutineScope.launch {
            coroutineScope {
                logger.debug { "Sending4" }
                nodeTransporter.broadcast(id, RequestVote(newTerm, id))
                    .map {
                        it.awaitAll().map { responseMessage ->
                            when (responseMessage) {
                                is RequestVoteResponse -> {
                                    responseMessage
                                }
                            }
                        }
                    }
                    .onRight { logger.debug { "Out4" }
                        dispatchMessage(RequestedVotingComplete(it)) }
            }
        }

        // Go into candidate state, vote for itself
        return state.copy(currentTerm = newTerm, status = NodeStatus.CANDIDATE, voteCastInTerm = newTerm)
    }

    private fun transitionToLeader(state: NodeState): NodeState {
        logger.debug { "Node $id transitioning to leader" }

        leaderHeartbeatJob = coroutineScope.launch {
            try {
                while (true) {
                    nodeTransporter.broadcast(id, AppendEntries(state.currentTerm, id, emptyList()))
                    delay(config.leaderHeartbeatInterval)
                }
            } catch (e: CancellationException) {
                logger.debug { "Transition cancelled: $e" }
            }
        }

        return state.copy(status = NodeStatus.LEADER)
    }
}