package pl.petergood.raft.log

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.datetime.Clock
import pl.petergood.raft.AppendEntries
import pl.petergood.raft.AppendEntriesResponse
import pl.petergood.raft.ResponseMessage
import pl.petergood.raft.async.broadcastTillMajorityRespond
import pl.petergood.raft.node.AsyncNodeSocket
import pl.petergood.raft.node.NodeState
import pl.petergood.raft.node.NodeStatus
import pl.petergood.raft.node.NodeTransporter

data class ReplicationResult(
    val wasCommitted: Boolean
)

val logger = KotlinLogging.logger { }

suspend fun replicateToNodes(logEntry: LogEntry,
                             leaderId: Int,
                             term: Int,
                             numberOfNodes: Int,
                             nodeTransporter: NodeTransporter,
                             coroutineScope: CoroutineScope): ReplicationResult {
    val message = AppendEntries(term, leaderId, listOf(logEntry), 0, 0)
    val responses = broadcastTillMajorityRespond(nodeTransporter, leaderId, numberOfNodes, message, coroutineScope)

    return ReplicationResult(false)
}

suspend fun handleNewEntries(nodeId: Int,
                             state: NodeState,
                             log: Log,
                             appendEntries: AppendEntries,
                             responseSocket: AsyncNodeSocket<ResponseMessage>): NodeState {
    if (appendEntries.term < state.currentTerm) {
        // reject AppendEntries due to stale term
        responseSocket.dispatch(AppendEntriesResponse(state.currentTerm, false))
        return state.copy(lastLeaderHeartbeat = Clock.System.now())
    }

    // if node was candidate but has lost election
    val newState = if (state.status != NodeStatus.FOLLOWER) {
        logger.debug { "Node $nodeId transitioning from ${state.status} to FOLLOWER" }
        state.copy(status = NodeStatus.FOLLOWER)
    } else state

    // leader has new term
    val newState1 = if (appendEntries.term > state.currentTerm) {
        logger.debug { "Node $nodeId moving from term ${state.currentTerm} to ${appendEntries.term}" }
        newState.copy(currentTerm = appendEntries.term)
    } else newState

    // log consistency check
    log.findFirstIndex(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
        .onNone {
            logger.debug { "Node $nodeId did not find matching index ${appendEntries.prevLogIndex} ${appendEntries.prevLogTerm}" }
            responseSocket.dispatch(AppendEntriesResponse(state.currentTerm, false))
            return newState1.copy(lastLeaderHeartbeat = Clock.System.now())
        }
        .onSome {
            // overwrite all subsequent log entries (or do nothing if logs are consistent)
            log.truncateFromIndex(it)
            log.appendEntries(appendEntries.entries)
        }

    responseSocket.dispatch(AppendEntriesResponse(newState1.currentTerm, true))
    return newState1.copy(lastLeaderHeartbeat = Clock.System.now())
}