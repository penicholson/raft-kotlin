package pl.petergood.raft.node

import pl.petergood.raft.Message

interface Node {
    suspend fun start()
    suspend fun stop()
    suspend fun dispatchMessage(message: Message)

    fun isRunning(): Boolean
}