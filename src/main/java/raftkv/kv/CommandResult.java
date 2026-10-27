package raftkv.kv;

import java.io.Serializable;

public record CommandResult(boolean success, String value, String message) implements Serializable {
    public static CommandResult ok(String value) {
        return new CommandResult(true, value, "ok");
    }

    public static CommandResult error(String message) {
        return new CommandResult(false, null, message);
    }
}

