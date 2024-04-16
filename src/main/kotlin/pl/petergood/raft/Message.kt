package pl.petergood.raft

import java.util.*

sealed class Message
class StopNode : Message()
class CheckTimeout: Message()
class ExternalMessage(
    val sender: UUID,
    val message: RaftMessage
) : Message()

sealed class RaftMessage

class AppendEntries(
    val term: Int,
    val leaderId: UUID,
    val entires: Array<Entry>
) : RaftMessage()

class RequestVote(
    val term: Int,
    val candidateId: Int
) : RaftMessage()