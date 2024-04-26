package pl.petergood.raft

import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import pl.petergood.raft.node.NodeState
import pl.petergood.raft.node.hasNodeWon
import pl.petergood.raft.node.shouldGrantVote

class VotingTest : DescribeSpec({
    describe("RequestedVotingComplete") {
        describe("when node wins election") {
            it("should return true") {
                val message = RequestedVotingComplete(
                    responses = listOf(
                        RequestVoteResponse(0, true),
                        RequestVoteResponse(0, true),
                        RequestVoteResponse(0, false),
                    )
                )

                message.hasNodeWon(3).shouldBeTrue()
            }
        }

        describe("when node looses election") {
            it("should return false") {
                val message = RequestedVotingComplete(
                    responses = listOf(
                        RequestVoteResponse(0, true),
                        RequestVoteResponse(0, false),
                        RequestVoteResponse(0, false),
                    )
                )

                message.hasNodeWon(3).shouldBeFalse()
            }
        }
    }

    describe("RequestVote") {
        describe("when node votes in favour") {
            it("should return true") {
                val state = NodeState(currentTerm = 2, voteCastInTerm = 1)
                val message = RequestVote(3, 0)

                message.shouldGrantVote(state).shouldBeTrue()
            }
        }

        describe("when node votes against") {
            describe("when term is outdated") {
                it("should return false") {
                    val state = NodeState(currentTerm = 3, voteCastInTerm = 1)
                    val message = RequestVote(3, 0)

                    message.shouldGrantVote(state).shouldBeFalse()
                }
            }

            describe("when node has already voted in this election cycle") {
                it("should return false") {
                    val state = NodeState(currentTerm = 4, voteCastInTerm = 3)
                    val message = RequestVote(3, 0)

                    message.shouldGrantVote(state).shouldBeFalse()
                }
            }
        }
    }
})