package pl.petergood.raft.node

import arrow.core.Option
import arrow.core.none
import arrow.core.some
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import pl.petergood.raft.*
import pl.petergood.raft.log.Log
import pl.petergood.raft.log.LogEntry
import pl.petergood.raft.log.handleNewEntries
import pl.petergood.raft.log.replicateToNodes
import pl.petergood.raft.voting.hasNodeWon
import pl.petergood.raft.voting.shouldGrantVote
import java.util.*
import kotlin.random.Random
import kotlin.system.exitProcess
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

    fun isRunning(): Boolean
    fun getId(): Int
    fun getStatus(): NodeStatus
    fun getLog(): Log

    suspend fun store(value: Any): Option<StorageError>
}

sealed class StorageError
data object NodeNotLeaderError : StorageError()

class RaftNode(
    private val id: Int,
    private val config: NodeConfig,
    private val nodeTransporter: NodeTransporter,
    private val nodeRegistry: NodeRegistry,
    private val log: Log
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

    // the coroutine scope all coroutines related to this node will be launched in
    private val coroutineScope = CoroutineScope(SupervisorJob())

    // main Node channel
    private val inputChannel: Channel<Message> = Channel()

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
                logger.debug { "Timeout job ($id) cancelled: $e" }
            }
        }

        mainJob = coroutineScope.launch {
            nodeRegistry.registerNode(id, SingleMachineChannelingNodeSocket(inputChannel, coroutineScope))

            handler()
        }
    }

    override suspend fun stop() {
        inputChannel.send(StopNode)
    }

    override fun isRunning(): Boolean = state.status != NodeStatus.STOPPED
    override fun getId(): Int = id
    override fun getStatus(): NodeStatus = state.status
    override fun getLog(): Log = log

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

                is StoreInLog ->
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
            is AppendEntries ->
                handleNewEntries(id, state, log, message, responseSocket)

            // vote request from another node
            is RequestVote ->
                message.handle(state, responseSocket)
        }
    }

    // invoked when a vote initiated by this node has been completed (all other nodes have voted)
    fun RequestedVotingComplete.handle(state: NodeState): NodeState {
        logger.debug { "Handling votes ($id): $responses" }

        // Transition to leader state or go back to follower
        return if (hasNodeWon(nodeRegistry.getNumberOfNodes())) transitionToLeader(state) else {
            logger.debug { "Transitioning ($id) from ${state.status} to FOLLOWER" }
            state.copy(status = NodeStatus.FOLLOWER)
        }
    }

    suspend fun StoreInLog.handle(state: NodeState): NodeState {
        if (state.status != NodeStatus.LEADER) {
            logger.error { "Fatal error - node $id with status ${state.status} trying to store value in log" }
            stop()
        }

        val logEntry = log.appendEntry(value, state.currentTerm)
        val res = replicateToNodes(logEntry, id, state.currentTerm, nodeRegistry.getNumberOfNodes(), nodeTransporter, coroutineScope)

        return state
    }

    // handle RequestVote
    suspend fun RequestVote.handle(state: NodeState, responseSocket: AsyncNodeSocket<ResponseMessage>): NodeState {
        if (shouldGrantVote(state)) {
            // got voting request from outdated node
            responseSocket.dispatch(RequestVoteResponse(state.currentTerm, false))
            return state
        }

        responseSocket.dispatch(RequestVoteResponse(state.currentTerm, true))
        return state.copy(voteCastInTerm = state.currentTerm)
    }

    // exposed function to add entry to replicated log
    override suspend fun store(value: Any): Option<StorageError> {
        if (getStatus() != NodeStatus.LEADER) {
            // TODO: maybe route this request to the leader?
            NodeNotLeaderError.some()
        }

        inputChannel.send(StoreInLog(value))

        return none()
    }

    private suspend fun startElection(state: NodeState): NodeState {
        logger.debug { "Starting election on $id" }
        val newTerm = state.currentTerm + 1

        coroutineScope.launch {
            nodeTransporter.broadcast(id, RequestVote(newTerm, id))
                .map {
                    it.awaitAll()
                        .map { responseMessage ->
                            when (responseMessage) {
                                is RequestVoteResponse -> {
                                    responseMessage
                                }
                                else -> {
                                    logger.error { "Fatal error - got illegal response $responseMessage" }
                                    //TODO: handle this in a better way
                                    throw IllegalArgumentException()
                                }
                            }
                        }
                }
                .onRight { inputChannel.send(RequestedVotingComplete(it)) }
        }

        // Go into candidate state, vote for itself
        return state.copy(currentTerm = newTerm, status = NodeStatus.CANDIDATE, voteCastInTerm = newTerm)
    }

    private fun transitionToLeader(state: NodeState): NodeState {
        logger.debug { "Transitioning ($id) from ${state.status} to LEADER" }

        leaderHeartbeatJob = coroutineScope.launch {
            try {
                while (true) {
                    nodeTransporter.broadcast(id, AppendEntries(state.currentTerm, id, emptyList(), 0, 0))
                    delay(config.leaderHeartbeatInterval)
                }
            } catch (e: CancellationException) {
                logger.debug { "Leadership heartbeat job ($id) cancelled: $e" }
            }
        }

        return state.copy(status = NodeStatus.LEADER)
    }
}