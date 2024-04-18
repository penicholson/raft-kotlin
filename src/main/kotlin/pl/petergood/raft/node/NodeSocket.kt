package pl.petergood.raft.node

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import pl.petergood.raft.ExternalMessage
import pl.petergood.raft.RaftMessage
import pl.petergood.raft.ResponseMessage
import java.util.concurrent.CompletableFuture

interface NodeSocket<T> {
    suspend fun dispatch(message: T): CompletableFuture<ResponseMessage>
}

interface AsyncNodeSocket<T> {
    suspend fun dispatch(message: T)
}

class SingleMachineResponseSocket(private val channel: Channel<ResponseMessage>) : AsyncNodeSocket<ResponseMessage> {
    override suspend fun dispatch(message: ResponseMessage) {
        coroutineScope {
            launch {
                channel.send(message)
            }
        }
    }
}

class SingleMachineChannelingNodeSocket(private val channel: Channel<ExternalMessage>) : NodeSocket<RaftMessage> {
    override suspend fun dispatch(message: RaftMessage): CompletableFuture<ResponseMessage> {
        val completableFuture = CompletableFuture<ResponseMessage>()

        coroutineScope {
            launch {
                val responseChannel: Channel<ResponseMessage> = Channel()
                val responseSocket = SingleMachineResponseSocket(responseChannel)
                val externalMessage = ExternalMessage(responseSocket, message)

                channel.send(externalMessage)

                val response = responseChannel.receive()
                completableFuture.complete(response)
            }
        }

        return completableFuture
    }
}