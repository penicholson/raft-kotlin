package pl.petergood.raft

import pl.petergood.raft.log.LogEntry
import pl.petergood.raft.node.AsyncNodeSocket

sealed class Message

data object StopNode : Message()

data object CheckTimeout: Message()

class RequestedVotingComplete(
    val responses: List<RequestVoteResponse>
): Message()

// internal message dispatched to leader node
data class StoreInLog(
    val value: Any
) : Message()

data class ExternalMessage(
    val responseSocket: AsyncNodeSocket<ResponseMessage>,
    val message: RaftMessage
) : Message()

sealed class RaftMessage

data class AppendEntries(
    val term: Int,
    val leaderId: Int,
    val entries: List<LogEntry>
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

data class AppendEntriesResponse(
    val term: Int,
    val success: Boolean
) : ResponseMessage()