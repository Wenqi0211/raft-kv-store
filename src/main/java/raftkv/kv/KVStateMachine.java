package raftkv.kv;

import java.util.LinkedHashMap;
import java.util.Map;

/** Applied only in Raft log order under the owning node lock. */
public final class KVStateMachine {
    private final LinkedHashMap<String,String> state = new LinkedHashMap<>();
    private final LinkedHashMap<String,ClientSession> sessions = new LinkedHashMap<>();

    public CommandResult apply(Command command) {
        if (command.clientId() != null && command.type() != Command.Type.NOOP) {
            ClientSession prior = sessions.get(command.clientId());
            if (prior != null && command.sequence() <= prior.sequence()) {
                if (command.sequence() < prior.sequence()) return CommandResult.error("stale client sequence; not executed");
                return prior.matches(command) ? prior.result()
                        : CommandResult.error("client sequence reused for a different operation; not executed");
            }
        }
        CommandResult result = switch(command.type()) {
            case PUT -> { state.put(command.key(),command.value()); yield CommandResult.ok(command.value()); }
            case GET -> CommandResult.ok(state.get(command.key()));
            case DELETE -> CommandResult.ok(state.remove(command.key()));
            case NOOP -> CommandResult.ok(null);
        };
        if (command.clientId() != null && command.type() != Command.Type.NOOP) {
            sessions.put(command.clientId(),new ClientSession(command.sequence(),command.type(),command.key(),command.value(),result));
        }
        return result;
    }
    public Map<String,String> snapshot() { return new LinkedHashMap<>(state); }
    public Map<String,ClientSession> sessionSnapshot() { return new LinkedHashMap<>(sessions); }
    public void restore(Map<String,String> data) { restore(data,Map.of()); }
    public void restore(Map<String,String> data,Map<String,ClientSession> savedSessions) {
        state.clear(); state.putAll(data); sessions.clear();
        if(savedSessions != null) sessions.putAll(savedSessions);
    }
    public int size() { return state.size(); }
}
