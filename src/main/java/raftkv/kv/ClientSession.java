package raftkv.kv;

import java.io.Serializable;
import java.util.Objects;

/** Durable latest response for one monotonically sequenced client session. */
public record ClientSession(long sequence, Command.Type type, String key, String value,
                            CommandResult result) implements Serializable {
    public boolean matches(Command command) {
        return type == command.type() && Objects.equals(key, command.key())
                && Objects.equals(value, command.value());
    }
}
