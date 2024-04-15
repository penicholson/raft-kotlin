package pl.petergood.raft.node

import pl.petergood.raft.Message

interface Node {
    fun start()
    suspend fun dispatchMessage(message: Message)
}