package raftkv.raft;

import raftkv.kv.Command;
import raftkv.kv.CommandResult;
import raftkv.kv.KVStateMachine;
import raftkv.kv.KvRpc;
import raftkv.rpc.HttpRpcClient;
import raftkv.rpc.RaftTransport;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class RaftNode {
    private static final Duration RPC_TIMEOUT = Duration.ofMillis(450);
    private static final int MAX_APPEND_BATCH = 64;

    private final Object lock = new Object();
    private final String id;
    private final String url;
    private final Map<String, Peer> peers;
    private final PersistentStore store;
    private final KVStateMachine stateMachine = new KVStateMachine();
    private final int snapshotThreshold;
    private final Random random;
    private final ElectionTiming timing;
    private final RaftTransport rpcClient;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService networkPool;
    private final Map<Long, PendingRequest> pendingResults = new LinkedHashMap<>();
    private final Set<String> replicationInFlight = ConcurrentHashMap.newKeySet();

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private String votedFor;
    private final List<LogEntry> log = new ArrayList<>();
    private long snapshotIndex;
    private long snapshotTerm;
    private Snapshot snapshot = Snapshot.empty();
    private long commitIndex;
    private long lastApplied;
    private String leaderId;
    private long electionDeadlineMillis;
    private volatile boolean running;
    private boolean stopped;
    private boolean failed;

    private final Map<String, Long> nextIndex = new LinkedHashMap<>();
    private final Map<String, Long> matchIndex = new LinkedHashMap<>();

    public RaftNode(String id, String url, Map<String, Peer> peers, Path dataDir, int snapshotThreshold) throws IOException {
        this(id,url,peers,dataDir,snapshotThreshold,new HttpRpcClient(),System.nanoTime());
    }

    public RaftNode(String id,String url,Map<String,Peer> peers,Path dataDir,int snapshotThreshold,
                    RaftTransport transport,long randomSeed) throws IOException {
        this(id,url,peers,dataDir,snapshotThreshold,transport,randomSeed,ElectionTiming.fastSimulation());
    }

    public RaftNode(String id,String url,Map<String,Peer> peers,Path dataDir,int snapshotThreshold,
                    RaftTransport transport,long randomSeed,ElectionTiming timing) throws IOException {
        this.timing=Objects.requireNonNull(timing);
        this.rpcClient = Objects.requireNonNull(transport);
        this.random = new Random(randomSeed);
        this.id = Objects.requireNonNull(id);
        this.url = Objects.requireNonNull(url);
        this.peers = new LinkedHashMap<>(peers);
        this.store = new PersistentStore(dataDir);
        this.snapshotThreshold = snapshotThreshold;
        this.scheduler = Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "raft-" + id + "-tick"));
        this.networkPool = Executors.newCachedThreadPool(runnable -> new Thread(runnable, "raft-" + id + "-rpc"));
        loadPersistentState();
        resetElectionDeadlineLocked();
    }

    public String id() {
        return id;
    }

    public String url() {
        return url;
    }

    public Role role() {
        synchronized (lock) {
            return role;
        }
    }

    public long currentTerm() {
        synchronized(lock) { return currentTerm; }
    }

    public long snapshotIndex() {
        synchronized (lock) {
            return snapshotIndex;
        }
    }

    public long lastAppliedIndex() {
        synchronized (lock) {
            return lastApplied;
        }
    }

    // A local diagnostic copy, not a client read or a linearizable read API.
    public Map<String, String> localState() {
        synchronized (lock) {
            return stateMachine.snapshot();
        }
    }

    public Map<String,raftkv.kv.ClientSession> localSessions() {
        synchronized(lock) { return stateMachine.sessionSnapshot(); }
    }

    public void start() {
        synchronized (lock) {
            ensureHealthyLocked();
            if (running) {
                throw new IllegalStateException("Node is already running");
            }
            running = true;
        }
        scheduler.scheduleWithFixedDelay(this::safeElectionTick, 25, 25, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::safeHeartbeatTick, 40, timing.heartbeatMillis(), TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::safeApplyTick, 10, 10, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        synchronized (lock) {
            running = false;
            stopped = true;
            role = Role.FOLLOWER;
            leaderId = null;
            failPendingLocked("node stopped before result; outcome unknown");
        }
        scheduler.shutdownNow();
        networkPool.shutdownNow();
        rpcClient.close();
    }

    public KvRpc.ClientCommandResponse submit(Command command) {
        PendingRequest pending;
        long index;
        synchronized (lock) {
            if (!running || role != Role.LEADER) {
                return KvRpc.ClientCommandResponse.redirect("not leader", leaderId, leaderUrlLocked());
            }
            index = lastLogIndexLocked() + 1;
            LogEntry entry = new LogEntry(index, currentTerm, command);
            log.add(entry);
            pending = new PendingRequest(currentTerm, command.requestId(), new CompletableFuture<>());
            pendingResults.put(index, pending);
            persistLocked();
            updateCommitIndexLocked();
        }
        replicateAllAsync();
        try {
            CommandResult result = pending.future().get(5, TimeUnit.SECONDS);
            synchronized (lock) {
                return KvRpc.ClientCommandResponse.ok(result, leaderId, leaderUrlLocked());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return abandonRequest(index, pending, "interrupted waiting for result; outcome unknown");
        } catch (TimeoutException e) {
            return abandonRequest(index, pending, "timed out waiting for commit; outcome unknown");
        } catch (java.util.concurrent.ExecutionException e) {
            return abandonRequest(index, pending, "request failed before result; outcome unknown");
        }
    }

    private KvRpc.ClientCommandResponse abandonRequest(long index, PendingRequest pending, String message) {
        synchronized (lock) {
            pendingResults.remove(index, pending);
            return KvRpc.ClientCommandResponse.error(message, leaderId, leaderUrlLocked());
        }
    }

    public RaftRpc.RequestVoteResponse handleRequestVote(RaftRpc.RequestVoteRequest request) {
        synchronized (lock) {
            ensureHealthyLocked();
            if (request.term() < currentTerm) {
                return new RaftRpc.RequestVoteResponse(currentTerm, false);
            }
            if (request.term() > currentTerm) {
                // A higher term must be durable and clear our vote, but a rejected
                // stale candidate is not leader contact and must not postpone
                // our own election. Granting a vote below resets the timer.
                becomeFollowerLocked(request.term(), null, false);
            }
            boolean upToDate = request.lastLogTerm() > lastLogTermLocked()
                    || (request.lastLogTerm() == lastLogTermLocked() && request.lastLogIndex() >= lastLogIndexLocked());
            boolean canVote = votedFor == null || votedFor.equals(request.candidateId());
            boolean granted = canVote && upToDate;
            if (granted) {
                votedFor = request.candidateId();
                resetElectionDeadlineLocked();
                persistLocked();
            }
            return new RaftRpc.RequestVoteResponse(currentTerm, granted);
        }
    }

    public RaftRpc.AppendEntriesResponse handleAppendEntries(RaftRpc.AppendEntriesRequest request) {
        synchronized (lock) {
            ensureHealthyLocked();
            if (request.term() < currentTerm) {
                return new RaftRpc.AppendEntriesResponse(currentTerm, false, lastLogIndexLocked());
            }
            if (request.term() > currentTerm || role != Role.FOLLOWER) {
                becomeFollowerLocked(request.term(), request.leaderId());
            }
            leaderId = request.leaderId();
            resetElectionDeadlineLocked();

            if (request.prevLogIndex() < snapshotIndex) {
                return new RaftRpc.AppendEntriesResponse(currentTerm, false, snapshotIndex);
            }
            if (request.prevLogIndex() > lastLogIndexLocked()) {
                return new RaftRpc.AppendEntriesResponse(currentTerm, false, lastLogIndexLocked());
            }
            if (termAtLocked(request.prevLogIndex()) != request.prevLogTerm()) {
                // A failed prefix check is not evidence that our suffix is wrong.
                return new RaftRpc.AppendEntriesResponse(currentTerm, false, Math.max(snapshotIndex, request.prevLogIndex() - 1));
            }

            long match = request.prevLogIndex();
            boolean changed = false;
            long expectedIndex = match + 1;
            for (LogEntry incoming : request.entries()) {
                if (incoming.index() != expectedIndex++) {
                    throw new IllegalArgumentException("AppendEntries must contain contiguous entries after prevLogIndex");
                }
            }
            for (LogEntry incoming : request.entries()) {
                LogEntry local = entryAtLocked(incoming.index());
                if (local != null && local.term() != incoming.term()) {
                    if (incoming.index() <= commitIndex) {
                        return new RaftRpc.AppendEntriesResponse(currentTerm, false, commitIndex);
                    }
                    deleteFromLocked(incoming.index());
                    local = null;
                    changed = true;
                }
                if (local == null) {
                    log.add(incoming);
                    changed = true;
                }
                match = incoming.index();
            }
            long confirmedCommit = Math.min(request.leaderCommit(), match);
            if (confirmedCommit > commitIndex) {
                // The local suffix beyond this RPC may still belong to an old leader.
                commitIndex = confirmedCommit;
                changed = true;
            }
            if (changed) {
                persistLocked();
            }
            return new RaftRpc.AppendEntriesResponse(currentTerm, true, match);
        }
    }

    public RaftRpc.InstallSnapshotResponse handleInstallSnapshot(RaftRpc.InstallSnapshotRequest request) {
        synchronized (lock) {
            ensureHealthyLocked();
            if (request.term() < currentTerm) {
                return new RaftRpc.InstallSnapshotResponse(currentTerm, false, snapshotIndex);
            }
            if (request.term() > currentTerm || role != Role.FOLLOWER) {
                becomeFollowerLocked(request.term(), request.leaderId());
            }
            leaderId = request.leaderId();
            resetElectionDeadlineLocked();
            if (request.lastIncludedIndex() <= lastApplied) {
                // Delayed snapshots must never roll back a state machine already ahead.
                return new RaftRpc.InstallSnapshotResponse(currentTerm, true,
                        Math.max(snapshotIndex, request.lastIncludedIndex()));
            }

            boolean keepSuffix = termAtLocked(request.lastIncludedIndex()) == request.lastIncludedTerm();
            if (!keepSuffix && request.lastIncludedIndex() <= commitIndex) {
                return new RaftRpc.InstallSnapshotResponse(currentTerm, false, snapshotIndex);
            }
            if (keepSuffix) {
                log.removeIf(entry -> entry.index() <= request.lastIncludedIndex());
            } else {
                log.clear();
            }
            snapshotIndex = request.lastIncludedIndex();
            snapshotTerm = request.lastIncludedTerm();
            snapshot = new Snapshot(snapshotIndex, snapshotTerm, request.data(),request.sessions());
            stateMachine.restore(snapshot.data(),snapshot.sessions());
            commitIndex = Math.max(commitIndex, snapshotIndex);
            lastApplied = snapshotIndex;
            persistLocked();
            return new RaftRpc.InstallSnapshotResponse(currentTerm, true, snapshotIndex);
        }
    }

    public String statusJson() {
        synchronized (lock) {
            return "{"
                    + "\"id\":\"" + escape(id) + "\","
                    + "\"url\":\"" + escape(url) + "\","
                    + "\"role\":\"" + role + "\","
                    + "\"running\":" + running + ","
                    + "\"term\":" + currentTerm + ","
                    + "\"leaderId\":" + nullable(leaderId) + ","
                    + "\"commitIndex\":" + commitIndex + ","
                    + "\"lastApplied\":" + lastApplied + ","
                    + "\"lastLogIndex\":" + lastLogIndexLocked() + ","
                    + "\"snapshotIndex\":" + snapshotIndex + ","
                    + "\"kvSize\":" + stateMachine.size()
                    + "}";
        }
    }

    private void safeElectionTick() {
        try {
            electionTick();
        } catch (RuntimeException e) {
            System.err.println(id + " election tick failed: " + e.getMessage());
        }
    }

    private void safeHeartbeatTick() {
        try {
            heartbeatTick();
        } catch (RuntimeException e) {
            System.err.println(id + " heartbeat tick failed: " + e.getMessage());
        }
    }

    private void safeApplyTick() {
        try {
            applyTick();
        } catch (RuntimeException e) {
            System.err.println(id + " apply tick failed: " + e.getMessage());
        }
    }

    private void electionTick() {
        if (!running) {
            return;
        }
        boolean shouldStart;
        synchronized (lock) {
            shouldStart = role != Role.LEADER && System.currentTimeMillis() >= electionDeadlineMillis;
        }
        if (shouldStart) {
            startElection();
        }
    }

    private void heartbeatTick() {
        if (!running) {
            return;
        }
        synchronized (lock) {
            if (role != Role.LEADER) {
                return;
            }
        }
        replicateAllAsync();
    }

    private void applyTick() {
        if (!running) {
            return;
        }
        List<PendingRequest> completed = new ArrayList<>();
        List<Long> completedIndexes = new ArrayList<>();
        List<CommandResult> results = new ArrayList<>();
        boolean changed = false;
        synchronized (lock) {
            if (!running) {
                return;
            }
            while (lastApplied < commitIndex) {
                long next = lastApplied + 1;
                LogEntry entry = entryAtLocked(next);
                if (entry == null) {
                    throw new IllegalStateException("Committed log entry is missing at index " + next);
                }
                CommandResult result = stateMachine.apply(entry.command());
                lastApplied = next;
                changed = true;
                PendingRequest pending = pendingResults.get(next);
                if (pending != null) {
                    completed.add(pending);
                    completedIndexes.add(next);
                    // An index can be reused after an uncommitted suffix is replaced.
                    boolean sameRequest = pending.term() == entry.term()
                            && Objects.equals(pending.requestId(), entry.command().requestId());
                    results.add(sameRequest ? result : CommandResult.error("log entry replaced; outcome unknown"));
                }
            }
            if (lastApplied - snapshotIndex >= snapshotThreshold) {
                createSnapshotLocked();
            } else if (changed) {
                persistLocked();
            }
            for (int i = 0; i < completed.size(); i++) {
                pendingResults.remove(completedIndexes.get(i), completed.get(i));
            }
        }
        for (int i = 0; i < completed.size(); i++) {
            completed.get(i).future().complete(results.get(i));
        }
    }

    private void startElection() {
        final long electionTerm;
        final long lastIndex;
        final long lastTerm;
        boolean wonImmediately = false;
        synchronized (lock) {
            // A heartbeat may have arrived after electionTick checked the deadline.
            if (!running || role == Role.LEADER || System.currentTimeMillis() < electionDeadlineMillis) {
                return;
            }
            role = Role.CANDIDATE;
            currentTerm++;
            votedFor = id;
            leaderId = null;
            electionTerm = currentTerm;
            lastIndex = lastLogIndexLocked();
            lastTerm = lastLogTermLocked();
            resetElectionDeadlineLocked();
            persistLocked();
            if (majority() == 1) {
                becomeLeaderLocked();
                wonImmediately = true;
            }
        }
        if (wonImmediately) {
            replicateAllAsync();
            return;
        }

        int[] votes = {1};
        for (Peer peer : otherPeers()) {
            networkPool.submit(() -> {
                try {
                    RaftRpc.RequestVoteRequest request = new RaftRpc.RequestVoteRequest(electionTerm, id, lastIndex, lastTerm);
                    RaftRpc.RequestVoteResponse response = rpcClient.postObject(peer.url(), "/rpc/requestVote", request,
                            RaftRpc.RequestVoteResponse.class, RPC_TIMEOUT);
                    boolean becameLeader = false;
                    synchronized (lock) {
                        if (!running) {
                            return;
                        }
                        if (response.term() > currentTerm) {
                            becomeFollowerLocked(response.term(), null);
                            return;
                        }
                        if (role != Role.CANDIDATE || currentTerm != electionTerm) {
                            return;
                        }
                        if (response.voteGranted()) {
                            votes[0]++;
                            if (votes[0] >= majority()) {
                                becomeLeaderLocked();
                                becameLeader = true;
                            }
                        }
                    }
                    if (becameLeader) {
                        replicateAllAsync();
                    }
                } catch (Exception ignored) {
                    // Election retries are driven by the next randomized timeout.
                }
            });
        }
    }

    private void replicateAllAsync() {
        if (!running) {
            return;
        }
        for (Peer peer : otherPeers()) {
            // One RPC stream per follower prevents delayed replies from undoing progress.
            if (replicationInFlight.add(peer.id())) {
                try {
                    networkPool.submit(() -> {
                        try {
                            replicatePeer(peer);
                        } finally {
                            replicationInFlight.remove(peer.id());
                        }
                    });
                } catch (RejectedExecutionException stopped) {
                    replicationInFlight.remove(peer.id());
                }
            }
        }
    }

    private void replicatePeer(Peer peer) {
        for (int attempts = 0; running && attempts < 100; attempts++) {
            ReplicationTask task;
            synchronized (lock) {
                if (role != Role.LEADER) {
                    return;
                }
                long next = nextIndex.getOrDefault(peer.id(), lastLogIndexLocked() + 1);
                if (next <= snapshotIndex) {
                    task = ReplicationTask.snapshot(peer, currentTerm, leaderIdOrSelfLocked(), snapshot);
                } else {
                    long prev = next - 1;
                    List<LogEntry> entries = entriesFromLocked(next, MAX_APPEND_BATCH);
                    task = ReplicationTask.append(peer, currentTerm, leaderIdOrSelfLocked(), prev, termAtLocked(prev), entries, commitIndex);
                }
            }

            try {
                if (task.snapshotRequest != null) {
                    RaftRpc.InstallSnapshotResponse response = rpcClient.postObject(peer.url(), "/rpc/installSnapshot",
                            task.snapshotRequest, RaftRpc.InstallSnapshotResponse.class, RPC_TIMEOUT);
                    synchronized (lock) {
                        if (!running) {
                            return;
                        }
                        if (response.term() > currentTerm) {
                            becomeFollowerLocked(response.term(), null);
                            return;
                        }
                        if (role != Role.LEADER || task.term != currentTerm) {
                            return;
                        }
                        if (response.success()) {
                            long confirmed = task.snapshotRequest.lastIncludedIndex();
                            matchIndex.merge(peer.id(), confirmed, Math::max);
                            nextIndex.put(peer.id(), matchIndex.get(peer.id()) + 1);
                            updateCommitIndexLocked();
                        }
                    }
                    continue;
                }

                RaftRpc.AppendEntriesResponse response = rpcClient.postObject(peer.url(), "/rpc/appendEntries",
                        task.appendRequest, RaftRpc.AppendEntriesResponse.class, RPC_TIMEOUT);
                synchronized (lock) {
                    if (!running) {
                        return;
                    }
                    if (response.term() > currentTerm) {
                        becomeFollowerLocked(response.term(), null);
                        return;
                    }
                    if (role != Role.LEADER || task.term != currentTerm) {
                        return;
                    }
                    if (response.success()) {
                        List<LogEntry> sent = task.appendRequest.entries();
                        long confirmed = sent.isEmpty() ? task.appendRequest.prevLogIndex()
                                : sent.get(sent.size() - 1).index();
                        matchIndex.merge(peer.id(), confirmed, Math::max);
                        nextIndex.put(peer.id(), matchIndex.get(peer.id()) + 1);
                        updateCommitIndexLocked();
                        if (nextIndex.get(peer.id()) <= lastLogIndexLocked()) {
                            continue;
                        }
                        return;
                    }
                    long knownMatch = matchIndex.getOrDefault(peer.id(), 0L);
                    long retry = Math.min(nextIndex.get(peer.id()) - 1, response.matchIndex() + 1);
                    // Permit crossing the compacted prefix so the next attempt sends a snapshot.
                    long next = Math.max(knownMatch + 1, Math.max(1, retry));
                    nextIndex.put(peer.id(), next);
                }
            } catch (Exception ignored) {
                return;
            }
        }
    }

    private void updateCommitIndexLocked() {
        long last = lastLogIndexLocked();
        for (long candidate = last; candidate > commitIndex; candidate--) {
            if (termAtLocked(candidate) != currentTerm) {
                continue;
            }
            int replicated = 1;
            for (Peer peer : otherPeers()) {
                if (matchIndex.getOrDefault(peer.id(), 0L) >= candidate) {
                    replicated++;
                }
            }
            if (replicated >= majority()) {
                commitIndex = candidate;
                persistLocked();
                return;
            }
        }
    }

    private void becomeLeaderLocked() {
        role = Role.LEADER;
        leaderId = id;
        long next = lastLogIndexLocked() + 1;
        for (Peer peer : otherPeers()) {
            nextIndex.put(peer.id(), next);
            matchIndex.put(peer.id(), 0L);
        }
        log.add(new LogEntry(next, currentTerm, Command.noop()));
        persistLocked();
        updateCommitIndexLocked();
    }

    private void becomeFollowerLocked(long term, String newLeaderId) {
        becomeFollowerLocked(term, newLeaderId, true);
    }

    private void becomeFollowerLocked(long term, String newLeaderId, boolean resetElectionTimer) {
        boolean higherTerm = term > currentTerm;
        if (higherTerm) {
            currentTerm = term;
            votedFor = null;
        }
        role = Role.FOLLOWER;
        leaderId = newLeaderId;
        if (resetElectionTimer) resetElectionDeadlineLocked();
        failPendingLocked("leadership changed before result; outcome unknown");
        if (higherTerm) {
            // Every higher-term path must save, even a rejected vote or empty heartbeat.
            persistLocked();
        }
    }

    private void failPendingLocked(String message) {
        pendingResults.values().forEach(pending -> pending.future().complete(CommandResult.error(message)));
        pendingResults.clear();
    }

    private void createSnapshotLocked() {
        long includedIndex = lastApplied;
        long includedTerm = termAtLocked(includedIndex);
        snapshot = new Snapshot(includedIndex, includedTerm, stateMachine.snapshot(),stateMachine.sessionSnapshot());
        snapshotIndex = includedIndex;
        snapshotTerm = includedTerm;
        log.removeIf(entry -> entry.index() <= snapshotIndex);
        persistLocked();
    }

    private void loadPersistentState() throws IOException {
        PersistentState state = store.load();
        currentTerm = state.currentTerm;
        votedFor = state.votedFor;
        snapshotIndex = state.snapshotIndex;
        snapshotTerm = state.snapshotTerm;
        commitIndex = state.commitIndex;
        snapshot = state.snapshot == null ? Snapshot.empty() : state.snapshot;
        log.clear();
        log.addAll(state.log == null ? List.of() : state.log);
        stateMachine.restore(snapshot.data(),snapshot.sessions());
        lastApplied = snapshotIndex;
        long replayUntil = Math.min(state.lastApplied, lastLogIndexLocked());
        while (lastApplied < replayUntil) {
            long next = lastApplied + 1;
            LogEntry entry = entryAtLocked(next);
            if (entry == null) {
                break;
            }
            stateMachine.apply(entry.command());
            lastApplied = next;
        }
        commitIndex = Math.max(commitIndex, lastApplied);
    }

    private void persistLocked() {
        PersistentState state = new PersistentState(
                currentTerm,
                votedFor,
                snapshotIndex,
                snapshotTerm,
                commitIndex,
                lastApplied,
                log,
                snapshot
        );
        try {
            store.save(state);
        } catch (IOException e) {
            // Continuing after a failed durable-state update could violate Raft's vote/log rules.
            running = false;
            failed = true;
            role = Role.FOLLOWER;
            leaderId = null;
            failPendingLocked("persistence failed; node stopped, outcome unknown");
            scheduler.shutdownNow();
            networkPool.shutdownNow();
            throw new IllegalStateException("Unable to persist Raft state for " + id, e);
        }
    }

    private void ensureHealthyLocked() {
        if (failed || stopped) {
            throw new IllegalStateException("Node is stopped; restart from durable state before accepting RPCs");
        }
    }

    private List<Peer> otherPeers() {
        return peers.values().stream()
                .filter(peer -> !peer.id().equals(id))
                .sorted(Comparator.comparing(Peer::id))
                .toList();
    }

    private int majority() {
        return peers.size() / 2 + 1;
    }

    private void resetElectionDeadlineLocked() {
        electionDeadlineMillis = System.currentTimeMillis() + timing.minimumMillis()
                + random.nextInt(timing.maximumExclusiveMillis()-timing.minimumMillis());
    }

    private long lastLogIndexLocked() {
        if (log.isEmpty()) {
            return snapshotIndex;
        }
        return log.get(log.size() - 1).index();
    }

    private long lastLogTermLocked() {
        if (log.isEmpty()) {
            return snapshotTerm;
        }
        return log.get(log.size() - 1).term();
    }

    private long termAtLocked(long index) {
        if (index == 0) {
            return 0;
        }
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        LogEntry entry = entryAtLocked(index);
        return entry == null ? -1 : entry.term();
    }

    private LogEntry entryAtLocked(long index) {
        if (index <= snapshotIndex) {
            return null;
        }
        long offset = index - snapshotIndex - 1;
        if (offset >= 0 && offset < log.size()) {
            LogEntry candidate = log.get((int) offset);
            if (candidate.index() == index) {
                return candidate;
            }
        }
        return log.stream().filter(entry -> entry.index() == index).findFirst().orElse(null);
    }

    private void deleteFromLocked(long index) {
        if (index <= commitIndex) {
            throw new IllegalStateException("Cannot truncate a committed entry at index " + index);
        }
        log.removeIf(entry -> entry.index() >= index);
        pendingResults.entrySet().removeIf(entry -> {
            if (entry.getKey() >= index) {
                entry.getValue().future().complete(CommandResult.error("log entry replaced; outcome unknown"));
                return true;
            }
            return false;
        });
    }

    private List<LogEntry> entriesFromLocked(long index, int maxEntries) {
        List<LogEntry> result = new ArrayList<>();
        for (LogEntry entry : log) {
            if (entry.index() >= index) {
                result.add(entry);
                if (result.size() >= maxEntries) {
                    break;
                }
            }
        }
        return result;
    }

    private String leaderUrlLocked() {
        if (leaderId == null) {
            return null;
        }
        if (leaderId.equals(id)) {
            return url;
        }
        Peer peer = peers.get(leaderId);
        return peer == null ? null : peer.url();
    }

    private String leaderIdOrSelfLocked() {
        return leaderId == null ? id : leaderId;
    }

    private static String nullable(String value) {
        return value == null ? "null" : "\"" + escape(value) + "\"";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private record PendingRequest(long term, String requestId, CompletableFuture<CommandResult> future) {
    }

    private record ReplicationTask(
            Peer peer,
            long term,
            RaftRpc.AppendEntriesRequest appendRequest,
            RaftRpc.InstallSnapshotRequest snapshotRequest
    ) {
        static ReplicationTask append(
                Peer peer,
                long term,
                String leaderId,
                long prevLogIndex,
                long prevLogTerm,
                List<LogEntry> entries,
                long leaderCommit
        ) {
            return new ReplicationTask(peer, term,
                    new RaftRpc.AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit),
                    null);
        }

        static ReplicationTask snapshot(Peer peer, long term, String leaderId, Snapshot snapshot) {
            return new ReplicationTask(peer, term, null,
                    new RaftRpc.InstallSnapshotRequest(term, leaderId, snapshot.lastIncludedIndex(),
                            snapshot.lastIncludedTerm(), snapshot.data(),snapshot.sessions()));
        }
    }
}
