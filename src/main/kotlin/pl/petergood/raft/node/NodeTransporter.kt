package pl.petergood.raft.node

import arrow.core.Either
import arrow.core.Option
import arrow.core.raise.either
import pl.petergood.raft.RaftMessage
import pl.petergood.raft.ResponseMessage
import java.util.*
import java.util.concurrent.CompletableFuture

interface NodeTransporter {
    suspend fun dispatch(nodeId: UUID, message: RaftMessage) : Either<Error, CompletableFuture<ResponseMessage>>
    suspend fun broadcast(currentNodeId: UUID, message: RaftMessage) : Either<Error, List<CompletableFuture<ResponseMessage>>>
}

class Error

class NodeTransporterImpl(private val nodeRegistry: NodeRegistry) : NodeTransporter {
    override suspend fun dispatch(nodeId: UUID, message: RaftMessage): Either<Error, CompletableFuture<ResponseMessage>> =
        Option.fromNullable(nodeRegistry.getNodeSocket(nodeId))
            .toEither { Error() }
            .map { it.dispatch(message) }

    override suspend fun broadcast(currentNodeId: UUID, message: RaftMessage): Either<Error, List<CompletableFuture<ResponseMessage>>> = either {
        nodeRegistry.getAllNodes()
            .filter { it != currentNodeId }
            .map { dispatch(it, message).bind() }
    }
}