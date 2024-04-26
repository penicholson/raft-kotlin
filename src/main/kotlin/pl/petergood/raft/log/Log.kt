package pl.petergood.raft.log

interface Log {
    fun appendEntry(value: Any): LogEntry
    fun getEntries(): List<LogEntry>
}

data class LogEntry(
    val index: Int,
    val committed: Boolean,
    val value: Any
)

class InMemoryLog : Log {
    private val entryList: MutableList<LogEntry> = mutableListOf()

    override fun appendEntry(value: Any): LogEntry {
        val entry = LogEntry(entryList.size, false, value)
        entryList.add(entry)

        return entry
    }

    override fun getEntries(): List<LogEntry> =
        entryList
}