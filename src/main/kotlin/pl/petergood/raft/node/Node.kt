package pl.petergood.raft.node

import kotlinx.coroutines.CoroutineScope
import pl.petergood.raft.Message
import java.util.*

interface Node {
    suspend fun start(coroutineScope: CoroutineScope)
    suspend fun stop()
    suspend fun dispatchMessage(message: Message)

    fun isRunning(): Boolean
    fun getId(): UUID
}