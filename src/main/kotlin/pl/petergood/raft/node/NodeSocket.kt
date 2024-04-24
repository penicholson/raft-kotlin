package pl.petergood.raft.node

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import pl.petergood.raft.ExternalMessage
import pl.petergood.raft.RaftMessage
import pl.petergood.raft.ResponseMessage

interface NodeSocket<T> {
    suspend fun dispatch(message: T): Deferred<ResponseMessage>
}

interface AsyncNodeSocket<T> {
    suspend fun dispatch(message: T)
}

class SingleMachineResponseSocket(private val channel: Channel<ResponseMessage>,
                                  private val coroutineScope: CoroutineScope) : AsyncNodeSocket<ResponseMessage> {
    override suspend fun dispatch(message: ResponseMessage) {
        coroutineScope.launch {
            channel.send(message)
        }
    }
}

class SingleMachineChannelingNodeSocket(private val channel: Channel<in ExternalMessage>,
                                        private val coroutineScope: CoroutineScope) : NodeSocket<RaftMessage> {
    private val logger = KotlinLogging.logger { }

    override suspend fun dispatch(message: RaftMessage): Deferred<ResponseMessage> {
        logger.debug { "Dispatching message $message" }

        return coroutineScope.async {
            val responseChannel: Channel<ResponseMessage> = Channel()
            val responseSocket = SingleMachineResponseSocket(responseChannel, coroutineScope)
            val externalMessage = ExternalMessage(responseSocket, message)

            channel.send(externalMessage)

            responseChannel.receive()
        }
    }
}