package pl.petergood.raft.node

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import pl.petergood.raft.ExternalMessage
import pl.petergood.raft.RaftMessage
import pl.petergood.raft.RequestVoteResponse
import pl.petergood.raft.ResponseMessage
import java.lang.Exception

interface NodeSocket<T> {
    suspend fun dispatch(message: T): Deferred<ResponseMessage>
}

interface AsyncNodeSocket<T> {
    suspend fun dispatch(message: T)
}

class SingleMachineResponseSocket(private val channel: Channel<ResponseMessage>,
                                  private val coroutineScope: CoroutineScope) : AsyncNodeSocket<ResponseMessage> {
                                      private val logger = KotlinLogging.logger {  }

    override suspend fun dispatch(message: ResponseMessage) {
        coroutineScope.launch {
            logger.debug { "Sending1" }
            channel.send(message)
            logger.debug { "Out1" }
        }
    }
}

class SingleMachineChannelingNodeSocket(private val channel: Channel<in ExternalMessage>,
                                        private val coroutineScope: CoroutineScope) : NodeSocket<RaftMessage> {
    private val logger = KotlinLogging.logger { }

    override suspend fun dispatch(message: RaftMessage): Deferred<ResponseMessage> {
        logger.debug { "Dispatching message $message" }

        return coroutineScope.async {
            try {
                val responseChannel: Channel<ResponseMessage> = Channel()
                val responseSocket = SingleMachineResponseSocket(responseChannel, coroutineScope)
                val externalMessage = ExternalMessage(responseSocket, message)

                logger.debug { "Sending2" }
                channel.send(externalMessage)
                logger.debug { "Out2" }

                logger.debug { "Sending3 $externalMessage" }
                val res = responseChannel.receive()
                logger.debug { "Out3" }
                res
            } catch (e: Exception) {
                logger.debug { "Cancelled3 $e" }
                throw e
            }
        }
    }
}