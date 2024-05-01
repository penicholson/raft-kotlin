package pl.petergood.raft.log

import arrow.core.*

interface Log {
    fun appendEntry(value: Any, term: Int): LogEntry
    fun appendEntries(entries: List<LogEntry>)
    fun truncateFromIndex(index: Int)
    fun getEntries(): List<LogEntry>
    fun getEntryAtIndex(index: Int): Either<LogError, LogEntry>
    fun findFirstIndex(prevLogIndex: Int, prevLogTerm: Int): Option<Int>
    fun getSize(): Int
}

sealed class LogError
data object IndexNotFound : LogError()

data class LogEntry(
    val index: Int,
    val term: Int,
    val committed: Boolean,
    val value: Any
)

class InMemoryLog(initialLog: List<LogEntry> = emptyList()) : Log {
    private val entryList: MutableList<LogEntry> = mutableListOf()

    init {
        entryList.addAll(initialLog)
    }

    override fun appendEntry(value: Any, term: Int): LogEntry {
        val entry = LogEntry(entryList.size, term, false, value)
        entryList.add(entry)

        return entry
    }

    override fun appendEntries(entries: List<LogEntry>) {
        entryList.addAll(entries)
    }

    override fun truncateFromIndex(index: Int) {
        entryList.removeAll { it.index > index }
    }

    override fun getEntries(): List<LogEntry> =
        entryList

    override fun getEntryAtIndex(index: Int): Either<LogError, LogEntry> {
        if (index < 0 || index >= entryList.size) {
            return IndexNotFound.left()
        }

        return entryList[index].right()
    }

    override fun findFirstIndex(prevLogIndex: Int, prevLogTerm: Int): Option<Int> {
        return getEntryAtIndex(prevLogIndex)
            .mapLeft { none<Int>() }
            .map { if (prevLogTerm == it.term) it.index.some() else none() }
            .merge()
    }

    override fun getSize(): Int = entryList.size
}