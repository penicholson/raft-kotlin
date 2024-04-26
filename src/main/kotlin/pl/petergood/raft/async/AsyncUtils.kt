package pl.petergood.raft.async

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import pl.petergood.raft.RaftMessage
import pl.petergood.raft.ResponseMessage
import pl.petergood.raft.node.NodeTransporter
import kotlin.math.ceil

suspend fun broadcastTillMajorityRespond(nodeTransporter: NodeTransporter,
                                  nodeId: Int,
                                  numberOfNodes: Int,
                                  message: RaftMessage,
                                  coroutineScope: CoroutineScope): List<ResponseMessage> {
    val resultChan = Channel<ResponseMessage>()
    val logger = KotlinLogging.logger { }

    nodeTransporter.broadcast(nodeId, message)
        .onRight {
            it.forEach {
                coroutineScope.launch {
                    try {
                        val res = it.await()
                        logger.debug { "Received response $res" }

                        resultChan.send(res)
                    } catch (e: Exception) {
                        logger.warn { "Dropped response due to $e" }
                    }
                }
            }
        }

    // new scope so that we wait for completion
    return coroutineScope {
        async {
            val results: MutableList<ResponseMessage> = mutableListOf()
            var receivedResults = 0

            //TODO: Timeouts
            while (receivedResults < ceil(numberOfNodes / 2.0)) {
                results.add(resultChan.receive())
                receivedResults++
            }

            logger.debug { "Received $receivedResults results ($results), closing channel" }

            resultChan.close()

            results
        }
    }.await()
}