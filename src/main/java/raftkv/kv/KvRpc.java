package raftkv.kv;

import java.io.Serializable;

public final class KvRpc {
    private KvRpc() {
    }

    public record ClientCommandRequest(Command command) implements Serializable {
    }

    public record ClientCommandResponse(
            boolean success,
            String value,
            String message,
            String leaderId,
            String leaderUrl
    ) implements Serializable {
        public static ClientCommandResponse ok(CommandResult result, String leaderId, String leaderUrl) {
            return new ClientCommandResponse(result.success(), result.value(), result.message(), leaderId, leaderUrl);
        }

        public static ClientCommandResponse redirect(String message, String leaderId, String leaderUrl) {
            return new ClientCommandResponse(false, null, message, leaderId, leaderUrl);
        }

        public static ClientCommandResponse error(String message, String leaderId, String leaderUrl) {
            return new ClientCommandResponse(false, null, message, leaderId, leaderUrl);
        }
    }
}

