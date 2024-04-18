package pl.petergood.raft.node

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
    override suspend fun dispatch(message: RaftMessage): Deferred<ResponseMessage> {
        return coroutineScope {
            async {
                val responseChannel: Channel<ResponseMessage> = Channel()
                val responseSocket = SingleMachineResponseSocket(responseChannel)
                val externalMessage = ExternalMessage(responseSocket, message)

                channel.send(externalMessage)

                responseChannel.receive()
            }
        }
    }
}