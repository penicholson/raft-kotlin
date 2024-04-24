package pl.petergood.raft

import pl.petergood.raft.node.AsyncNodeSocket
import pl.petergood.raft.node.NodeSocket
import java.util.*

sealed class Message
data object StopNode : Message()
data object CheckTimeout: Message()
class RequestedVotingComplete(
    val responses: List<RequestVoteResponse>
): Message()
data class ExternalMessage(
    val responseSocket: AsyncNodeSocket<ResponseMessage>,
    val message: RaftMessage
) : Message()

sealed class RaftMessage

data class AppendEntries(
    val term: Int,
    val leaderId: UUID,
    val entires: Array<Entry>
) : RaftMessage()

data class RequestVote(
    val term: Int,
    val candidateId: Int
) : RaftMessage()

sealed class ResponseMessage

data class RequestVoteResponse(
    val term: Int,
    val voteGranted: Boolean
) : ResponseMessage()