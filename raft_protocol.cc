#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    // Your code here
    m << args.term;
    m << args.candidateId;
    m << args.lastLogIndex;
    m << args.lastLogTerm;

    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    // Your code here
    u >> args.term;
    u >> args.candidateId;
    u >> args.lastLogIndex;
    u >> args.lastLogTerm;

    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    // Your code here
    m << reply.term;
    m << reply.voteGranted;

    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    // Your code here
    u >> reply.term;
    u >> reply.voteGranted;

    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m << reply.term;
    m << reply.success;

    return m;
}
unmarshall& operator>>(unmarshall &u, append_entries_reply& reply) {
    // Your code here
    u >> reply.term;
    u >> reply.success;

    return u;
}

marshall& operator<<(marshall &m, const install_snapshot_args& args) {
    // Your code here
    m << args.term;
    m << args.leaderId;
    m << args.lastIncludedIndex;
    m << args.lastIncludedTerm;
    m << args.offset;
    m << args.data;
    m << args.done;

    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
    // Your code here
    u >> args.term;
    u >> args.leaderId;
    u >> args.lastIncludedIndex;
    u >> args.lastIncludedTerm;
    u >> args.offset;
    u >> args.data;
    u >> args.done;

    return u; 
}

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here
    m << reply.term;

    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here
    u >> reply.term;

    return u;
}