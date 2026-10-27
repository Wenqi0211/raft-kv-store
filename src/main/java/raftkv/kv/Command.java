package raftkv.kv;

import java.io.Serializable;
import java.util.UUID;

public record Command(Type type, String key, String value, String requestId,
                      String clientId, long sequence) implements Serializable {
    public enum Type { PUT, GET, DELETE, NOOP }

    // Legacy unsequenced commands retain their original behavior.
    public Command(Type type, String key, String value, String requestId) {
        this(type, key, value, requestId, null, 0);
    }
    public Command {
        if (type == null || requestId == null) throw new IllegalArgumentException("type/requestId required");
        if (clientId != null && (clientId.isBlank() || sequence < 1))
            throw new IllegalArgumentException("sequenced commands require a client ID and positive sequence");
    }
    public static Command client(Type type, String key, String value, String clientId, long sequence) {
        return new Command(type, key, value, clientId + ":" + sequence, clientId, sequence);
    }
    public static Command put(String key, String value) { return new Command(Type.PUT,key,value,UUID.randomUUID().toString()); }
    public static Command get(String key) { return new Command(Type.GET,key,null,UUID.randomUUID().toString()); }
    public static Command delete(String key) { return new Command(Type.DELETE,key,null,UUID.randomUUID().toString()); }
    public static Command noop() { return new Command(Type.NOOP,"",null,UUID.randomUUID().toString()); }
}
