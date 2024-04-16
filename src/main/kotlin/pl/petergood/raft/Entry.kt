package pl.petergood.raft

data class Entry(
    val index: Int,
    val value: Any
)