package raftkv.client;

import raftkv.kv.KvRpc;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ClientMain {
    private ClientMain() {
    }

    public static void main(String[] args) {
        ParsedArgs parsed = parse(args);
        try (KvClient client = new KvClient(parsed.nodes())) {
            if (parsed.command().equals("status")) {
                client.statuses().forEach(System.out::println);
                return;
            }

            KvRpc.ClientCommandResponse response = switch (parsed.command()) {
                case "put" -> client.put(required(parsed.positionals(), 0, "key"),
                        required(parsed.positionals(), 1, "value"));
                case "get" -> client.get(required(parsed.positionals(), 0, "key"));
                case "delete" -> client.delete(required(parsed.positionals(), 0, "key"));
                default -> throw new IllegalArgumentException("Unknown command: " + parsed.command());
            };
            if (response.success()) {
                System.out.println(response.value() == null ? "OK" : response.value());
            } else {
                System.err.println(response.message());
                System.exit(2);
            }
        }
    }

    private static ParsedArgs parse(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        int i = 0;
        while (i < args.length && args[i].startsWith("--")) {
            String key = args[i++];
            if (i >= args.length) {
                throw new IllegalArgumentException("Missing value for " + key);
            }
            options.put(key, args[i++]);
        }
        if (i >= args.length) {
            usage();
            throw new IllegalArgumentException("Missing command");
        }
        String command = args[i++];
        List<String> positionals = Arrays.asList(Arrays.copyOfRange(args, i, args.length));
        String nodeText = options.getOrDefault("--nodes",
                "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003");
        List<String> nodes = Arrays.stream(nodeText.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
        return new ParsedArgs(nodes, command, positionals);
    }

    private static String required(List<String> values, int index, String name) {
        if (index >= values.size()) {
            throw new IllegalArgumentException("Missing " + name);
        }
        return values.get(index);
    }

    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  client --nodes http://127.0.0.1:8001,http://127.0.0.1:8002 put key value");
        System.out.println("  client --nodes http://127.0.0.1:8001,http://127.0.0.1:8002 get key");
        System.out.println("  client status");
    }

    private record ParsedArgs(List<String> nodes, String command, List<String> positionals) {
    }
}

