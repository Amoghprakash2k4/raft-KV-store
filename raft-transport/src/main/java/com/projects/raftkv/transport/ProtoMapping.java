package com.projects.raftkv.transport;

import com.google.protobuf.ByteString;
import com.projects.raftkv.core.Bytes;
import com.projects.raftkv.core.EntryPayload;
import com.projects.raftkv.core.LogEntry;
import com.projects.raftkv.core.Message;
import com.projects.raftkv.core.NodeId;
import com.projects.raftkv.proto.RaftMessage;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link Message} ↔ protobuf {@link RaftMessage} (raft.proto, DESIGN.md M5-B). Every field is copied both
 * ways; {@code ProtoMappingProperties} checks that {@code fromProto(toProto(m)).equals(m)} for every kind of
 * message. Decoding goes through the records' own constructors, so a malformed message from the network is
 * rejected with {@link IllegalArgumentException} instead of reaching the core.
 */
public final class ProtoMapping {
    private ProtoMapping() {}

    public static RaftMessage toProto(Message m) {
        RaftMessage.Builder b = RaftMessage.newBuilder().setFrom(m.from().value()).setTo(m.to().value()).setTerm(m.term());
        switch (m) {
            case Message.RequestVote rv -> b.setRequestVote(com.projects.raftkv.proto.RequestVote.newBuilder()
                    .setLastLogIndex(rv.lastLogIndex()).setLastLogTerm(rv.lastLogTerm()));
            case Message.RequestVoteResponse r -> b.setRequestVoteResponse(
                    com.projects.raftkv.proto.RequestVoteResponse.newBuilder().setVoteGranted(r.voteGranted()));
            case Message.PreVote pv -> b.setPreVote(com.projects.raftkv.proto.PreVote.newBuilder()
                    .setLastLogIndex(pv.lastLogIndex()).setLastLogTerm(pv.lastLogTerm()));
            case Message.PreVoteResponse r -> b.setPreVoteResponse(
                    com.projects.raftkv.proto.PreVoteResponse.newBuilder().setGranted(r.granted()));
            case Message.AppendEntries ae -> {
                var ab = com.projects.raftkv.proto.AppendEntries.newBuilder()
                        .setPrevLogIndex(ae.prevLogIndex()).setPrevLogTerm(ae.prevLogTerm())
                        .setLeaderCommit(ae.leaderCommit()).setReadRound(ae.readRound());
                for (LogEntry e : ae.entries()) ab.addEntries(toProto(e));
                b.setAppendEntries(ab);
            }
            case Message.AppendEntriesResponse r -> b.setAppendEntriesResponse(
                    com.projects.raftkv.proto.AppendEntriesResponse.newBuilder()
                            .setSuccess(r.success()).setMatchIndex(r.matchIndex()).setRejectedIndex(r.rejectedIndex())
                            .setConflictTerm(r.conflictTerm()).setConflictIndex(r.conflictIndex())
                            .setReadRound(r.readRound()));
            case Message.InstallSnapshot is -> b.setInstallSnapshot(com.projects.raftkv.proto.InstallSnapshot.newBuilder()
                    .setLastIncludedIndex(is.lastIncludedIndex()).setLastIncludedTerm(is.lastIncludedTerm())
                    .setOffset(is.offset()).setData(ByteString.copyFrom(is.data().toByteArray()))
                    .setDone(is.done()).setReadRound(is.readRound()));
            case Message.InstallSnapshotResponse r -> b.setInstallSnapshotResponse(
                    com.projects.raftkv.proto.InstallSnapshotResponse.newBuilder()
                            .setLastIncludedIndex(r.lastIncludedIndex()).setNextOffset(r.nextOffset())
                            .setDone(r.done()).setReadRound(r.readRound()));
        }
        return b.build();
    }

    /** @throws IllegalArgumentException if the message has no body or a field is out of range */
    public static Message fromProto(RaftMessage p) {
        NodeId from = new NodeId(p.getFrom());
        NodeId to = new NodeId(p.getTo());
        long term = p.getTerm();
        return switch (p.getBodyCase()) {
            case REQUEST_VOTE -> new Message.RequestVote(from, to, term,
                    p.getRequestVote().getLastLogIndex(), p.getRequestVote().getLastLogTerm());
            case REQUEST_VOTE_RESPONSE -> new Message.RequestVoteResponse(from, to, term,
                    p.getRequestVoteResponse().getVoteGranted());
            case PRE_VOTE -> new Message.PreVote(from, to, term,
                    p.getPreVote().getLastLogIndex(), p.getPreVote().getLastLogTerm());
            case PRE_VOTE_RESPONSE -> new Message.PreVoteResponse(from, to, term, p.getPreVoteResponse().getGranted());
            case APPEND_ENTRIES -> {
                var ae = p.getAppendEntries();
                List<LogEntry> entries = new ArrayList<>(ae.getEntriesCount());
                for (var e : ae.getEntriesList()) entries.add(fromProto(e));
                yield new Message.AppendEntries(from, to, term, ae.getPrevLogIndex(), ae.getPrevLogTerm(), entries,
                        ae.getLeaderCommit(), ae.getReadRound());
            }
            case APPEND_ENTRIES_RESPONSE -> {
                var r = p.getAppendEntriesResponse();
                yield new Message.AppendEntriesResponse(from, to, term, r.getSuccess(), r.getMatchIndex(),
                        r.getRejectedIndex(), r.getConflictTerm(), r.getConflictIndex(), r.getReadRound());
            }
            case INSTALL_SNAPSHOT -> {
                var is = p.getInstallSnapshot();
                yield new Message.InstallSnapshot(from, to, term, is.getLastIncludedIndex(), is.getLastIncludedTerm(),
                        is.getOffset(), Bytes.copyOf(is.getData().toByteArray()), is.getDone(), is.getReadRound());
            }
            case INSTALL_SNAPSHOT_RESPONSE -> {
                var r = p.getInstallSnapshotResponse();
                yield new Message.InstallSnapshotResponse(from, to, term, r.getLastIncludedIndex(), r.getNextOffset(),
                        r.getDone(), r.getReadRound());
            }
            case BODY_NOT_SET -> throw new IllegalArgumentException("RaftMessage without a body from " + from);
        };
    }

    static com.projects.raftkv.proto.LogEntry toProto(LogEntry e) {
        var b = com.projects.raftkv.proto.LogEntry.newBuilder().setIndex(e.index()).setTerm(e.term());
        switch (e.payload()) {
            case EntryPayload.Noop n -> b.setNoop(com.projects.raftkv.proto.Noop.getDefaultInstance());
            case EntryPayload.Command c -> b.setCommand(ByteString.copyFrom(c.data().toByteArray()));
        }
        return b.build();
    }

    static LogEntry fromProto(com.projects.raftkv.proto.LogEntry e) {
        EntryPayload payload = switch (e.getPayloadCase()) {
            case NOOP -> new EntryPayload.Noop();
            case COMMAND -> new EntryPayload.Command(Bytes.copyOf(e.getCommand().toByteArray()));
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("log entry " + e.getIndex() + " without a payload");
        };
        return new LogEntry(e.getIndex(), e.getTerm(), payload);
    }
}
