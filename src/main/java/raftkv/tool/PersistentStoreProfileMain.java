package raftkv.tool;

import raftkv.kv.ClientSession;
import raftkv.kv.Command;
import raftkv.kv.CommandResult;
import raftkv.raft.PersistentState;
import raftkv.raft.PersistentStore;
import raftkv.raft.Snapshot;

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Diagnostic only: representative saved-state I/O phases, not Raft throughput. */
public final class PersistentStoreProfileMain {
    private PersistentStoreProfileMain() { }
    public static void main(String[] args) throws Exception {
        if(args.length!=1)throw new IllegalArgumentException("Usage: PersistentStoreProfileMain NEW_REPORT_DIRECTORY");
        Path report=Path.of(args[0]).toAbsolutePath();
        if(Files.exists(report))throw new IllegalArgumentException("Refusing to replace "+report);
        Files.createDirectories(report);
        Map<String,String> data=new LinkedHashMap<>();
        for(int i=0;i<1000;i++){String digits=Integer.toString(i);data.put("k"+i,"v"+"0".repeat(63-digits.length())+digits);}
        Map<String,ClientSession> sessions=new LinkedHashMap<>();
        for(int i=0;i<4;i++)sessions.put("client-"+i,new ClientSession(250,Command.Type.PUT,"k"+i,data.get("k"+i),CommandResult.ok(data.get("k"+i))));
        PersistentState state=new PersistentState(1,"n1",1000,1,1000,1000,List.of(),new Snapshot(1000,1,data,sessions));
        StringBuilder csv=new StringBuilder("trial,mode,keys,value_bytes,channel_stream_write_calls,bytes,directory_preflight_ms,serialize_and_flush_ms,data_force_ms,rename_ms,directory_force_ms,total_ms\n");
        for(int trial=0;trial<6;trial++) {
            boolean[] order=trial%2==0?new boolean[]{false,true}:new boolean[]{true,false};
            for(boolean buffered:order) {
                Path directory=report.resolve((buffered?"buffered-":"unbuffered-")+trial);Files.createDirectories(directory);
                Measurement measurement=save(directory,state,buffered);
                PersistentState loaded=new PersistentStore(directory).load();
                if(!loaded.snapshot.data().equals(data)||!loaded.snapshot.sessions().equals(sessions))throw new AssertionError("state roundtrip differs");
                csv.append(String.format(Locale.ROOT,"%d,%s,1000,64,%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f%n",
                        trial,buffered?"buffered64k":"unbuffered",measurement.writes(),measurement.bytes(),
                        ms(measurement.preflight()),ms(measurement.serialization()),ms(measurement.dataForce()),
                        ms(measurement.rename()),ms(measurement.directoryForce()),ms(measurement.total())));
            }
        }
        Path output=report.resolve("phases.csv");Files.writeString(output,csv.toString());
        Files.writeString(report.resolve("scope.txt"),"Isolated persistence diagnostic using representative 1000-key/64-byte-value snapshots and four sessions. Six interleaved trials per mode. Both modes force the temporary data file, atomically replace the state file and force its directory; both also retain the existing directory preflight force. This is not a Raft client performance result.\n");
        System.out.print(csv);System.out.println("Diagnostic CSV: "+output);
    }
    private static Measurement save(Path directory,PersistentState state,boolean buffered) throws IOException {
        long start=System.nanoTime();long preflight,serialized,dataForced,renamed,directoryForced;long writes,bytes;
        try(FileChannel directoryChannel=FileChannel.open(directory,StandardOpenOption.READ)) {
            directoryChannel.force(true);preflight=System.nanoTime();
            Path temporary=Files.createTempFile(directory,"state-",".tmp");
            try {
                try(FileChannel channel=FileChannel.open(temporary,StandardOpenOption.WRITE)) {
                    CountingOutputStream raw=new CountingOutputStream(Channels.newOutputStream(channel));
                    OutputStream output=buffered?new BufferedOutputStream(raw,64*1024):raw;
                    ObjectOutputStream objects=new ObjectOutputStream(output);
                    objects.writeObject(state);objects.flush();serialized=System.nanoTime();
                    writes=raw.writes;bytes=raw.bytes;
                    channel.force(true);dataForced=System.nanoTime();
                }
                Files.move(temporary,directory.resolve("raft-state.bin"),StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);renamed=System.nanoTime();
                directoryChannel.force(true);directoryForced=System.nanoTime();
            } finally {Files.deleteIfExists(temporary);}
        }
        return new Measurement(writes,bytes,preflight-start,serialized-preflight,dataForced-serialized,
                renamed-dataForced,directoryForced-renamed,System.nanoTime()-start);
    }
    private static double ms(long nanos){return nanos/1e6;}
    private record Measurement(long writes,long bytes,long preflight,long serialization,long dataForce,long rename,long directoryForce,long total){ }
    private static final class CountingOutputStream extends OutputStream {
        private final OutputStream delegate;long writes,bytes;
        CountingOutputStream(OutputStream delegate){this.delegate=delegate;}
        @Override public void write(int value)throws IOException{writes++;bytes++;delegate.write(value);}
        @Override public void write(byte[] value,int offset,int length)throws IOException{writes++;bytes+=length;delegate.write(value,offset,length);}
        @Override public void flush()throws IOException{delegate.flush();}
    }
}
