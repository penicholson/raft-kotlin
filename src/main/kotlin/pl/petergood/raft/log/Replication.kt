package pl.petergood.raft.log

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import pl.petergood.raft.node.NodeRegistry

suspend fun replicateToNodes(logEntry: LogEntry, leaderId: Int, nodeRegistry: NodeRegistry) {
    // new coroutine scope - we want to block the RaftNode mainJob coroutine, but not the thread
    coroutineScope {
        launch {

        }
    }
}