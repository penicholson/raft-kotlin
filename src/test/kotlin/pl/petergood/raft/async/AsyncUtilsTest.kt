package pl.petergood.raft.async

import io.github.oshai.kotlinlogging.KotlinLogging
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.equals.shouldBeEqual
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import pl.petergood.raft.ExternalMessage
import pl.petergood.raft.RequestVote
import pl.petergood.raft.RequestVoteResponse
import pl.petergood.raft.ResponseMessage
import pl.petergood.raft.node.NodeTransporterImpl
import pl.petergood.raft.node.SingleMachineChannelingNodeSocket
import pl.petergood.raft.node.SingleMachineNodeRegistry

class AsyncUtilsTest : DescribeSpec({
    val logger = KotlinLogging.logger { }

    describe("broadcastTillMajorityResponds") {
        describe("when majority responds") {
            it("should wait till majority responds and then return their responses") {
                val coroutineScope = CoroutineScope(SupervisorJob())
                val nodeRegistry = SingleMachineNodeRegistry()
                val nodeChannels = List(4) { Channel<ExternalMessage>() }
                nodeChannels.forEachIndexed { id, chan ->
                    nodeRegistry.registerNode(id, SingleMachineChannelingNodeSocket(chan, coroutineScope)) }

                val nodeTransporter = NodeTransporterImpl(nodeRegistry)

                // Two node channels will respond
                nodeChannels.slice(1..2)
                    .forEachIndexed { i, chan ->
                        coroutineScope.launch {
                            val msg = chan.receive()
                            logger.debug { "Channel $i got $msg" }

                            msg.responseSocket.dispatch(RequestVoteResponse(0, false))
                        }
                    }

                val message = RequestVote(0, 0)
                val responses = broadcastTillMajorityRespond(nodeTransporter, 0, 4, message, coroutineScope)

                responses.size shouldBeEqual 2
            }
        }

        describe("when response qualifier is specified") {
            it("should only respect responses that fulfil qualifier") {
                val coroutineScope = CoroutineScope(SupervisorJob())
                val nodeRegistry = SingleMachineNodeRegistry()
                val nodeChannels = List(4) { Channel<ExternalMessage>() }
                nodeChannels.forEachIndexed { id, chan ->
                    nodeRegistry.registerNode(id, SingleMachineChannelingNodeSocket(chan, coroutineScope)) }

                val nodeTransporter = NodeTransporterImpl(nodeRegistry)

                val nodeVotes = listOf(false, true, false, true)

                // All node channels will respond, two with correct, two with incorrect
                nodeChannels
                    .forEachIndexed { i, chan ->
                        coroutineScope.launch {
                            val msg = chan.receive()
                            logger.debug { "Channel $i got $msg" }

                            msg.responseSocket.dispatch(RequestVoteResponse(0, nodeVotes[i]))
                        }
                    }

                val message = RequestVote(0, 0)
                val responseQualifier: (ResponseMessage) -> Boolean = { when (it) {
                    is RequestVoteResponse -> it.voteGranted
                    else -> false
                } }

                val responses = broadcastTillMajorityRespond(nodeTransporter, 0, 4, message, coroutineScope, responseQualifier)

                responses.size shouldBeEqual 2
            }
        }
    }
})