package pl.petergood.raft.node

import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import pl.petergood.raft.ExternalMessage
import pl.petergood.raft.RequestVote
import pl.petergood.raft.RequestVoteResponse
import pl.petergood.raft.ResponseMessage
import java.util.*

@OptIn(DelicateCoroutinesApi::class)
class NodeSocketTest : DescribeSpec({
    describe("SingleMachineChannelingNodeSocket") {
        it("should dispatch message and wait for response") {
            val chan = Channel<ExternalMessage>()
            val socket = SingleMachineChannelingNodeSocket(chan, GlobalScope)
            val message = RequestVote(0, 0)

            launch {
                val msg = chan.receive()
                msg.message shouldBeEqual message
                msg.responseSocket.dispatch(RequestVoteResponse(0, false))
            }

            val future = socket.dispatch(message)
            val responseMessage = future.await()
            responseMessage.shouldBeInstanceOf<RequestVoteResponse>()
        }
    }

    describe("SingleMachineResponseSocket") {
        it("should dispatch message and return immediately") {
            val chan = Channel<ResponseMessage>()
            val socket = SingleMachineResponseSocket(chan, GlobalScope)

            runBlocking {
                launch {
                    val msg = chan.receive()
                    msg.shouldBeInstanceOf<RequestVoteResponse>()
                }

                socket.dispatch(RequestVoteResponse(0, false))
            }
        }
    }
})