package pl.petergood.raft

sealed class Message
class StopNode : Message()
