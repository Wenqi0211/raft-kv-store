package raftkv.client;

import raftkv.kv.Command;
import raftkv.kv.KvRpc;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Command-line gRPC client; explicit identity options allow replaying one logical request. */
public final class GrpcClientMain {
    private GrpcClientMain() { }
    public static void main(String[] args) {
        int status=run(args);
        if(status!=0)System.exit(status);
    }
    static int run(String[] args) {
        Map<String,String> options=new LinkedHashMap<>();int index=0;
        Set<String> allowed=Set.of("--nodes","--client-id","--sequence");
        while(index<args.length&&args[index].startsWith("--")) {
            String key=args[index++];
            if(!allowed.contains(key)||index==args.length)throw new IllegalArgumentException("Expected known --option value");
            if(options.put(key,args[index++])!=null)throw new IllegalArgumentException("Duplicate option "+key);
        }
        if(index==args.length)throw new IllegalArgumentException("Expected put key value, get key, delete key, or status");
        String operation=args[index++];List<String> values=Arrays.asList(Arrays.copyOfRange(args,index,args.length));
        int required=switch(operation){case "put"->2;case "get","delete"->1;case "status"->0;default->throw new IllegalArgumentException("Unknown operation "+operation);};
        if(values.size()!=required)throw new IllegalArgumentException(operation+" requires "+required+" arguments");
        boolean explicit=options.containsKey("--client-id");
        if(explicit!=options.containsKey("--sequence"))throw new IllegalArgumentException("--client-id and --sequence must be supplied together");
        if(explicit&&operation.equals("status"))throw new IllegalArgumentException("status has no request sequence");
        List<String> nodes=Arrays.stream(options.getOrDefault("--nodes",
                "grpc://127.0.0.1:8101,grpc://127.0.0.1:8102,grpc://127.0.0.1:8103,grpc://127.0.0.1:8104,grpc://127.0.0.1:8105").split(","))
                .map(String::trim).filter(s->!s.isEmpty()).toList();
        try(GrpcKvClient client=new GrpcKvClient(nodes)) {
            if(operation.equals("status")){client.statuses().forEach(System.out::println);return 0;}
            KvRpc.ClientCommandResponse response;
            if(explicit) {
                Command.Type type=Command.Type.valueOf(operation.toUpperCase(java.util.Locale.ROOT));
                response=client.execute(Command.client(type,values.get(0),operation.equals("put")?values.get(1):null,
                        options.get("--client-id"),Long.parseLong(options.get("--sequence"))));
            } else {
                response=switch(operation) {
                    case "put"->client.put(values.get(0),values.get(1));
                    case "get"->client.get(values.get(0));
                    default->client.delete(values.get(0));
                };
            }
            if(!response.success()){System.err.println(response.message());return 2;}
            System.out.println(response.value()==null?"(null)":response.value());return 0;
        }
    }
}
