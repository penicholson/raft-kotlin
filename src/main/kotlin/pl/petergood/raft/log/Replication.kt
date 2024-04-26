package pl.petergood.raft.log

import kotlinx.coroutines.CoroutineScope
import pl.petergood.raft.AppendEntries
import pl.petergood.raft.async.broadcastTillMajorityRespond
import pl.petergood.raft.node.NodeTransporter

data class ReplicationResult(
    val wasCommitted: Boolean
)

suspend fun replicateToNodes(logEntry: LogEntry,
                             leaderId: Int,
                             term: Int,
                             numberOfNodes: Int,
                             nodeTransporter: NodeTransporter,
                             coroutineScope: CoroutineScope): ReplicationResult {
    val message = AppendEntries(term, leaderId, listOf(logEntry))
    val responses = broadcastTillMajorityRespond(nodeTransporter, leaderId, numberOfNodes, message, coroutineScope)


}