package raftkv.tool;

import raftkv.kv.KVStateMachine;
import raftkv.raft.PersistentStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Read-only inspection of atomically saved node state during a local run. */
public final class StateInspectMain {
    public static void main(String[] args) throws Exception {
        if(args.length!=1)throw new IllegalArgumentException("Expected a directory containing node directories");
        try(var children=Files.list(Path.of(args[0]))) {
            for(Path directory:children.filter(Files::isDirectory).sorted(Comparator.naturalOrder()).toList()) {
                Path file=directory.resolve("raft-state.bin");
                if(!Files.exists(file)){System.out.println(directory.getFileName()+": no saved state");continue;}
                var saved=new PersistentStore(directory).load();
                KVStateMachine state=new KVStateMachine();state.restore(saved.snapshot.data(),saved.snapshot.sessions());
                for(var entry:saved.log)if(entry.index()>saved.snapshotIndex&&entry.index()<=saved.lastApplied)state.apply(entry.command());
                int highest=state.snapshot().keySet().stream().filter(k->k.matches("k[0-9]+"))
                        .mapToInt(k->Integer.parseInt(k.substring(1))).max().orElse(-1);
                System.out.println(directory.getFileName()+": keys="+state.size()+", highest_preload_key="+highest
                        +", term="+saved.currentTerm+", commit="+saved.commitIndex+", applied="+saved.lastApplied
                        +", snapshot="+saved.snapshotIndex+", log_entries="+saved.log.size()
                        +", file_bytes="+Files.size(file)+", modified="+Files.getLastModifiedTime(file));
            }
        }
    }
}
