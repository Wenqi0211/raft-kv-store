package raftkv.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import raftkv.kv.ClientSession;
import raftkv.kv.Command;
import raftkv.kv.CommandResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BufferedPersistenceTest {
    @TempDir Path directory;

    @Test void bufferingKeepsSerializationBytesAndSnapshotSessionsCompatible() throws Exception {
        Map<String,String> data=new LinkedHashMap<>();
        for(int i=0;i<1000;i++)data.put("key-"+i,"x".repeat(80)+i);
        Map<String,ClientSession> sessions=Map.of("client",new ClientSession(12,Command.Type.DELETE,"removed",null,CommandResult.ok("original")));
        PersistentState state=new PersistentState(4,"n2",1000,4,1000,1000,List.of(),new Snapshot(1000,4,data,sessions));
        ByteArrayOutputStream expected=new ByteArrayOutputStream();
        try(ObjectOutputStream objects=new ObjectOutputStream(expected)){objects.writeObject(state);}
        new PersistentStore(directory).save(state);
        assertArrayEquals(expected.toByteArray(),Files.readAllBytes(directory.resolve("raft-state.bin")));
        var loaded=new PersistentStore(directory).load();
        assertEquals(data,loaded.snapshot.data());assertEquals(sessions,loaded.snapshot.sessions());
    }

    @Test void failureAfterBufferHasSpilledCannotReplacePreviousDurableState() throws Exception {
        PersistentStore store=new PersistentStore(directory);
        PersistentState original=new PersistentState(3,"n1",0,0,0,0,List.of(),Snapshot.empty());store.save(original);
        byte[] previous=Files.readAllBytes(directory.resolve("raft-state.bin"));
        PersistentState invalid=new PersistentState(4,"n2",0,0,0,0,List.of(),Snapshot.empty());invalid.log=new SpillingFailureLog();
        assertThrows(IOException.class,()->store.save(invalid));
        assertArrayEquals(previous,Files.readAllBytes(directory.resolve("raft-state.bin")));
        assertEquals(3,store.load().currentTerm);
        try(var files=Files.list(directory)){assertEquals(List.of("raft-state.bin"),files.map(p->p.getFileName().toString()).toList());}
    }

    private static final class SpillingFailureLog extends ArrayList<LogEntry> {
        private static final long serialVersionUID=1;
        private void writeObject(ObjectOutputStream output)throws IOException {
            output.write(new byte[160*1024]);output.flush();
            throw new IOException("injected failure after multiple output-buffer spills");
        }
    }
}
