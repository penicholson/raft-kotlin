package pl.petergood.raft.node

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import pl.petergood.raft.Message
import pl.petergood.raft.StopNode
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class RaftNode(val id: UUID) : Node {
    val inputChannel: Channel<Message> = Channel()
    val isRunning = AtomicBoolean(false)

    override suspend fun start() {
        isRunning.compareAndSet(false, true)
        handler()
    }

    override suspend fun stop() {
        inputChannel.send(StopNode())
    }

    override suspend fun dispatchMessage(message: Message) {
        inputChannel.send(message)
    }

    private suspend fun handler() {
        while (isRunning.get()) {
            val message = inputChannel.receive()

            when (message) {
                is StopNode -> {
                    isRunning.compareAndSet(true, false)
                }
            }
        }
    }
}

fun RaftNode.launch(scope: CoroutineScope) {
    scope.launch {
        start()
    }
}