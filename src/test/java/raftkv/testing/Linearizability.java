package raftkv.testing;

import raftkv.kv.Command;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Exhaustive bounded-history checker, honoring invocation/response real-time order. */
public final class Linearizability {
    public record Operation(long invoked,long completed,Command.Type type,String key,String value,String returned) { }
    public static boolean check(List<Operation> history) {
        if(history.size()>12) throw new IllegalArgumentException("bounded checker supports at most12 operations");
        return search(history,0,new HashMap<>());
    }
    private static boolean search(List<Operation> history,int done,Map<String,String> state) {
        if(done==(1<<history.size())-1)return true;
        for(int i=0;i<history.size();i++) {
            if((done&(1<<i))!=0)continue;
            Operation next=history.get(i);boolean eligible=true;
            for(int j=0;j<history.size();j++) {
                if(i!=j&&(done&(1<<j))==0&&history.get(j).completed()<next.invoked()) {eligible=false;break;}
            }
            if(!eligible)continue;
            Map<String,String> copy=new HashMap<>(state);
            String expected=switch(next.type()) {
                case PUT->{copy.put(next.key(),next.value());yield next.value();}
                case GET->copy.get(next.key());
                case DELETE->copy.remove(next.key());
                case NOOP->null;
            };
            if(Objects.equals(expected,next.returned())&&search(history,done|(1<<i),copy))return true;
        }
        return false;
    }
}
