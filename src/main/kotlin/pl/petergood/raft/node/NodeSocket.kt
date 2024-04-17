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

class SingleMachineResponseSocket(private val channel: Channel<ResponseMessage>,
                                  private val coroutineScope: CoroutineScope) : NodeSocket<ResponseMessage> {
    override suspend fun dispatch(message: ResponseMessage): CompletableFuture<ResponseMessage> {
        coroutineScope {
            launch {
                channel.send(message)
            }
        }
    }
}

class SingleMachineChannelingNodeSocket(private val channel: Channel<ExternalMessage>,
                                        private val coroutineScope: CoroutineScope) : NodeSocket<RaftMessage> {
    override suspend fun dispatch(message: RaftMessage): CompletableFuture<ResponseMessage> {
        val completableFuture = CompletableFuture<ResponseMessage>()

        coroutineScope {
            launch {
                val responseChannel: Channel<ResponseMessage> = Channel()
                val responseSocket = SingleMachineResponseSocket(responseChannel, coroutineScope)
                val externalMessage = ExternalMessage(responseSocket, message)

                channel.send(externalMessage)

                val response = responseChannel.receive()
                completableFuture.complete(response)
            }
        }

        return completableFuture
    }
}