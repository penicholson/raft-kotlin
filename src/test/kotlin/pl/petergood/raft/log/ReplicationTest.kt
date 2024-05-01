package pl.petergood.raft.log

import io.kotest.core.spec.style.DescribeSpec
import io.kotest.core.spec.style.describeSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.mockk.*
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import pl.petergood.raft.AppendEntries
import pl.petergood.raft.AppendEntriesResponse
import pl.petergood.raft.ResponseMessage
import pl.petergood.raft.node.AsyncNodeSocket
import pl.petergood.raft.node.NodeState
import pl.petergood.raft.node.NodeStatus

fun newEntriesTest(outcome: String,
                   nodeState: NodeState,
                   message: AppendEntries,
                   currentLogEntries: List<Pair<Int, Int>>,
                   expectedResponseTerm: Int,
                   expectedSuccess: Boolean,
                   expectedState: NodeState,
                   expectedLog: List<Pair<Int, Int>>) = describeSpec {
    it(outcome) {
        val log = InMemoryLog(currentLogEntries.map { LogEntry(it.first, it.second, true, 0) })

        val responseSocket: AsyncNodeSocket<ResponseMessage> = mockk()
        coJustRun { responseSocket.dispatch(AppendEntriesResponse(expectedResponseTerm, expectedSuccess)) }

        val newState = handleNewEntries(0, nodeState, log, message, responseSocket)

        newState shouldBeEqual expectedState
        log.getEntries().map { Pair(it.index, it.term) } shouldBeEqual expectedLog
        coVerify { responseSocket.dispatch(AppendEntriesResponse(expectedResponseTerm, expectedSuccess)) }
    }
}

class ReplicationTest : DescribeSpec({
    val currentInstant = Instant.parse("2024-05-01T11:31:10Z")

    beforeEach {
        mockkObject(Clock.System)
        every { Clock.System.now() } returns currentInstant
    }

    include(newEntriesTest(
        "should append entries to log",
        NodeState(currentTerm = 2, status = NodeStatus.FOLLOWER),
        AppendEntries(
            2, 0, listOf(
                LogEntry(3, 2, false, 0),
                LogEntry(4, 2, false, 0),
                LogEntry(5, 2, false, 0)
            ), 2, 2),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2)),
        2, true,
        NodeState(currentTerm = 2, status = NodeStatus.FOLLOWER, lastLeaderHeartbeat = currentInstant),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2), Pair(3, 2), Pair(4, 2), Pair(5, 2)),
    ))

    include(newEntriesTest(
        "should append entries to empty log",
        NodeState(currentTerm = 1, status = NodeStatus.FOLLOWER),
        AppendEntries(
            1, 0, listOf(
                LogEntry(0, 1, false, 0),
                LogEntry(1, 1, false, 0),
                LogEntry(2, 1, false, 0)
            ), -1, -1),
        emptyList(),
        1, true,
        NodeState(currentTerm = 1, status = NodeStatus.FOLLOWER, lastLeaderHeartbeat = currentInstant),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 1))
    ))

    include(newEntriesTest(
        "should truncate log and append entries",
        NodeState(currentTerm = 2, status = NodeStatus.FOLLOWER),
        AppendEntries(
            3, 0, listOf(
                LogEntry(3, 3, false, 0),
                LogEntry(4, 3, false, 0),
                LogEntry(5, 3, false, 0)
            ), 2, 2),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2), Pair(3, 2)),
        3, true,
        NodeState(currentTerm = 3, status = NodeStatus.FOLLOWER, lastLeaderHeartbeat = currentInstant),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2), Pair(3, 3), Pair(4, 3), Pair(5, 3)),
    ))

    include(newEntriesTest(
        "should reject when term outdated",
        NodeState(currentTerm = 2, status = NodeStatus.FOLLOWER),
        AppendEntries(
            1, 0, listOf(
                LogEntry(3, 1, false, 0),
                LogEntry(4, 1, false, 0),
                LogEntry(5, 1, false, 0)
            ), 2, 2),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2)),
        2, false,
        NodeState(currentTerm = 2, status = NodeStatus.FOLLOWER),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2)),
    ))

    include(newEntriesTest(
        "should update lastHeartbeat and do noting to log during heartbeat from leader",
        NodeState(currentTerm = 2, status = NodeStatus.FOLLOWER),
        AppendEntries(
            2, 0, emptyList(), 2, 2),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2)),
        2, true,
        NodeState(currentTerm = 2, status = NodeStatus.FOLLOWER, lastLeaderHeartbeat = currentInstant),
        listOf(Pair(0, 1), Pair(1, 1), Pair(2, 2)),
    ))
})