package pl.petergood.raft.node

import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import pl.petergood.raft.RaftMessage
import pl.petergood.raft.RequestVote
import pl.petergood.raft.RequestVoteResponse
import java.util.*
import java.util.concurrent.CompletableFuture

class NodeTransporterTest : DescribeSpec({
    var nodeRegistry: NodeRegistry = SingleMachineNodeRegistry()
    var nodeSocks: List<NodeSocket<RaftMessage>> = listOf()
    var nodeIds: List<UUID> = listOf()

    beforeEach {
        nodeRegistry = SingleMachineNodeRegistry()
        nodeSocks = List(3) { mockk<NodeSocket<RaftMessage>>() }
        nodeIds = List(3) { UUID.randomUUID() }

        nodeSocks.forEachIndexed { i, _ -> nodeRegistry.registerNode(nodeIds[i], nodeSocks[i]) }
    }

    describe("dispatch") {
        describe("when node is found") {
            it("dispatches message to correct node") {
                val nodeTransporter = NodeTransporterImpl(nodeRegistry)
                val msg = RequestVote(0, UUID.randomUUID())

                coEvery { nodeSocks[1].dispatch(msg) } returns CompletableFuture.completedFuture(RequestVoteResponse())

                val response = nodeTransporter.dispatch(nodeIds[1], msg)

                coVerify { nodeSocks[1].dispatch(msg) }
                response.isRight().shouldBeTrue()
                response.onRight { it.get().shouldBeInstanceOf<RequestVoteResponse>() }
            }
        }

        describe("when node is not found") {
            it("returns error") {
                val nodeTransporter = NodeTransporterImpl(nodeRegistry)
                val msg = RequestVote(0, UUID.randomUUID())

                val response = nodeTransporter.dispatch(UUID.randomUUID(), msg)

                response.isLeft().shouldBeTrue()
                response.onLeft { it.shouldBeInstanceOf<Error>() }
            }
        }
    }

    describe("broadcast") {
        describe("when no errors occur") {
            it("broadcasts message to all nodes except for sender") {
                val nodeTransporter = NodeTransporterImpl(nodeRegistry)
                val msg = RequestVote(0, UUID.randomUUID())

                coEvery { nodeSocks[0].dispatch(msg) } returns CompletableFuture.completedFuture(RequestVoteResponse())
                coEvery { nodeSocks[2].dispatch(msg) } returns CompletableFuture.completedFuture(RequestVoteResponse())

                val response = nodeTransporter.broadcast(nodeIds[1], msg)
                response.isRight().shouldBeTrue()
                response.onRight { it.forEach { res -> res.get().shouldBeInstanceOf<RequestVoteResponse>() } }

                coVerify { nodeSocks[0].dispatch(msg) }
                coVerify { nodeSocks[1].dispatch(msg) wasNot Called }
                coVerify { nodeSocks[2].dispatch(msg) }
            }
        }

        describe("when an error occurs") {
            it("returns error") {
                // TODO
            }
        }
    }
})