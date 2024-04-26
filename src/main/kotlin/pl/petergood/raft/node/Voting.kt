package pl.petergood.raft.node

import pl.petergood.raft.RequestVote
import pl.petergood.raft.RequestedVotingComplete
import kotlin.math.ceil

fun RequestedVotingComplete.hasNodeWon(numberOfNodes: Int): Boolean {
    val votesPerTerm = responses.groupBy { it.term }
    // Under typical circumstances this should be currentTerm - 1
    val prevTerm = votesPerTerm.keys.maxOf { it }

    return votesPerTerm[prevTerm]
        ?.count { it.voteGranted }

        // +1 is the current node
        ?.let { it + 1 > ceil(numberOfNodes / 2.0)} ?: false
}

fun RequestVote.shouldGrantVote(state: NodeState): Boolean = term > state.currentTerm && state.voteCastInTerm < term