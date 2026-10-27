package raftkv.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import raftkv.kv.Command;
import raftkv.kv.CommandResult;
import raftkv.kv.KvRpc;
import raftkv.rpc.RaftTransport;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class GrpcKvClientRetryTest {
    private static final String A="grpc://127.0.0.1:9101",B="grpc://127.0.0.1:9102";
    @AfterEach void clearThreadInterrupt(){Thread.interrupted();}

    @Test void retriesRedirectAndIoWithoutChangingLogicalIdentityOrPayload() {
        Stub transport=new Stub((attempt,command,timeout)->{
            if(attempt==1)return KvRpc.ClientCommandResponse.redirect("not leader","n2",B);
            if(attempt==2)throw new IOException("temporary connection failure");
            return ok(command.value());
        });
        Command logical=Command.client(Command.Type.PUT,"k","value","stable-client",42);
        try(GrpcKvClient client=client(transport,Duration.ofSeconds(1),Duration.ofMillis(300))) {
            assertEquals("value",client.execute(logical).value());
            assertEquals(List.of(A,B,B),transport.urls);
            assertEquals(3,transport.commands.size());
            transport.commands.forEach(command->{
                assertSame(logical,command);
                assertEquals("stable-client",command.clientId());assertEquals(42,command.sequence());
                assertEquals("stable-client:42",command.requestId());assertEquals("value",command.value());
            });
            GrpcKvClient.Stats stats=client.stats();
            assertEquals(1,stats.logicalRequests());assertEquals(1,stats.successfulRequests());
            assertEquals(0,stats.failedRequests());assertEquals(3,stats.rpcAttempts());
            assertEquals(2,stats.retryRpcAttempts());assertTrue(stats.backoffNanos()>0);
        }
        assertEquals(1,transport.closes);
    }

    @Test void automaticOperationsShareOneClientAndAdvanceSequenceOnlyBetweenCalls() {
        Stub transport=new Stub((attempt,command,timeout)->ok(command.value()));
        try(GrpcKvClient client=client(transport,Duration.ofSeconds(1),Duration.ofMillis(300))) {
            client.put("k","one");client.put("k","two");client.get("k");
            assertEquals(3,transport.commands.size());
            String id=transport.commands.get(0).clientId();
            for(int i=0;i<3;i++) {
                assertEquals(id,transport.commands.get(i).clientId());assertEquals(i+1,transport.commands.get(i).sequence());
            }
            assertEquals(3,client.stats().logicalRequests());assertEquals(3,client.stats().successfulRequests());
            assertEquals(3,client.stats().rpcAttempts());assertEquals(0,client.stats().retryRpcAttempts());
        }
    }

    @Test void repeatedNotLeaderRepliesFailWithinTotalBudgetInsteadOfTwoArbitraryRounds() {
        Stub transport=new Stub((attempt,command,timeout)->KvRpc.ClientCommandResponse.redirect("not leader",null,null));
        try(GrpcKvClient client=new GrpcKvClient(List.of(A),transport,Duration.ofMillis(90),Duration.ofMillis(60),3)) {
            long began=System.nanoTime();var response=client.put("k","value");
            long elapsed=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-began);
            assertFalse(response.success());assertTrue(response.message().contains("deadline"));
            assertTrue(elapsed>=70,"must actually exercise retry/backoff budget");
            assertTrue(elapsed<700,"bounded client budget must not turn into unbounded retries");
            assertTrue(client.stats().rpcAttempts()>=2);assertEquals(1,client.stats().failedRequests());
            assertEquals(client.stats().rpcAttempts()-1,client.stats().retryRpcAttempts());
        }
    }

    @Test void eachRpcGetsOnlyRemainingBudgetAndThatBudgetShrinks() {
        Stub transport=new Stub((attempt,command,timeout)->{
            Thread.sleep(20);throw new IOException("no ready leader");
        });
        try(GrpcKvClient client=new GrpcKvClient(List.of(A),transport,Duration.ofMillis(160),Duration.ofMillis(250),4)) {
            assertFalse(client.put("k","v").success());
            assertTrue(transport.timeouts.size()>=2);
            long prior=Long.MAX_VALUE;
            for(Duration timeout:transport.timeouts) {
                assertTrue(timeout.toNanos()>0);assertTrue(timeout.compareTo(Duration.ofMillis(160))<=0);
                assertTrue(timeout.toNanos()<prior,"remaining logical deadline should shrink on each retry");
                prior=timeout.toNanos();
            }
        }
    }

    @Test void rpcCapIsRespectedEvenWhenTotalBudgetIsLonger() {
        Stub transport=new Stub((attempt,command,timeout)->ok("v"));
        try(GrpcKvClient client=new GrpcKvClient(List.of(A),transport,Duration.ofSeconds(2),Duration.ofMillis(25),5)) {
            assertTrue(client.put("k","v").success());
            assertEquals(Duration.ofMillis(25),transport.timeouts.get(0));
        }
    }

    @Test void staleAndConflictingMetadataAreTerminalAndDoNotWasteRetryBudget() {
        for(String message:List.of("stale client sequence; not executed",
                "client sequence reused for a different operation; not executed","client sequence payload conflict")) {
            Stub transport=new Stub((attempt,command,timeout)->KvRpc.ClientCommandResponse.error(message,"n2",B));
            try(GrpcKvClient client=client(transport,Duration.ofSeconds(1),Duration.ofMillis(300))) {
                var response=client.execute(Command.client(Command.Type.DELETE,"k",null,"c",1));
                assertFalse(response.success());assertEquals(message,response.message());
                assertEquals(1,client.stats().rpcAttempts());assertEquals(0,client.stats().retryRpcAttempts());
                assertEquals(0,client.stats().backoffNanos());assertEquals(1,client.stats().failedRequests());
            }
        }
    }

    @Test void interruptionFromRpcPreservesFlagAndStopsImmediately() {
        Stub transport=new Stub((attempt,command,timeout)->{throw new InterruptedException("cancelled");});
        try(GrpcKvClient client=client(transport,Duration.ofSeconds(1),Duration.ofMillis(300))) {
            var response=client.put("k","v");assertFalse(response.success());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1,client.stats().rpcAttempts());assertEquals(1,client.stats().failedRequests());
        } finally {Thread.interrupted();}
    }

    @Test void alreadyInterruptedCallerDoesNotSendAnyRpc() {
        Stub transport=new Stub((attempt,command,timeout)->ok("v"));
        try(GrpcKvClient client=client(transport,Duration.ofSeconds(1),Duration.ofMillis(300))) {
            Thread.currentThread().interrupt();assertFalse(client.put("k","v").success());
            assertTrue(Thread.currentThread().isInterrupted());assertEquals(0,transport.commands.size());
            assertEquals(0,client.stats().rpcAttempts());assertEquals(1,client.stats().failedRequests());
        } finally {Thread.interrupted();}
    }

    @Test void interruptionDuringBackoffDoesNotRestartOrHideTheFailure() throws Exception {
        CountDownLatch attempted=new CountDownLatch(1);AtomicBoolean interruptedFlag=new AtomicBoolean();
        AtomicReference<KvRpc.ClientCommandResponse> returned=new AtomicReference<>();
        Stub transport=new Stub((attempt,command,timeout)->{attempted.countDown();throw new IOException("offline");});
        try(GrpcKvClient client=new GrpcKvClient(List.of(A),transport,Duration.ofSeconds(2),Duration.ofMillis(300),8)) {
            Thread worker=new Thread(()->{returned.set(client.put("k","v"));interruptedFlag.set(Thread.currentThread().isInterrupted());});
            worker.start();assertTrue(attempted.await(1,TimeUnit.SECONDS));worker.interrupt();worker.join(700);
            assertFalse(worker.isAlive());assertNotNull(returned.get());assertFalse(returned.get().success());
            assertTrue(interruptedFlag.get());assertEquals(1,client.stats().logicalRequests());
            assertEquals(1,client.stats().failedRequests());
        }
    }

    @Test void redirectsOutsideFixedMembershipAreNotFollowed() {
        Stub transport=new Stub((attempt,command,timeout)->attempt==1
                ?KvRpc.ClientCommandResponse.redirect("not leader","unknown","grpc://127.0.0.1:9999"):ok("v"));
        try(GrpcKvClient client=client(transport,Duration.ofSeconds(1),Duration.ofMillis(300))) {
            assertTrue(client.put("k","v").success());assertEquals(List.of(A,B),transport.urls);
        }
    }

    @Test void lateSuccessAfterBudgetIsReportedAsUnknownInsteadOfUnboundedSuccess() {
        Stub transport=new Stub((attempt,command,timeout)->{Thread.sleep(50);return ok("v");});
        try(GrpcKvClient client=client(transport,Duration.ofMillis(20),Duration.ofMillis(100))) {
            var result=client.put("k","v");assertFalse(result.success());assertTrue(result.message().contains("outcome unknown"));
            assertEquals(1,client.stats().rpcAttempts());assertEquals(1,client.stats().failedRequests());
        }
    }

    @Test void closedClientAndUnexpectedExceptionsStillCountFailedLogicalCalls() {
        Stub closed=new Stub((attempt,command,timeout)->ok("v"));
        GrpcKvClient client=client(closed,Duration.ofSeconds(1),Duration.ofMillis(300));
        client.close();client.close();assertFalse(client.put("k","v").success());
        assertEquals(1,closed.closes);assertEquals(0,client.stats().rpcAttempts());assertEquals(1,client.stats().failedRequests());
        Stub exploding=new Stub((attempt,command,timeout)->{throw new IllegalStateException("unexpected transport bug");});
        try(GrpcKvClient other=client(exploding,Duration.ofSeconds(1),Duration.ofMillis(300))) {
            assertThrows(IllegalStateException.class,()->other.put("k","v"));
            assertEquals(1,other.stats().logicalRequests());assertEquals(1,other.stats().failedRequests());assertEquals(1,other.stats().rpcAttempts());
        }
    }

    @Test void validatesGrpcEndpointsAndFinitePositiveBudgets() {
        Stub transport=new Stub((attempt,command,timeout)->ok("v"));
        for(String bad:List.of("http://127.0.0.1:1","grpc://127.0.0.1:0","grpc://127.0.0.1:70000",
                "grpc://user@127.0.0.1:1","grpc://127.0.0.1:1/path","grpc://127.0.0.1:1?query"))
            assertThrows(IllegalArgumentException.class,()->new GrpcKvClient(List.of(bad),transport,Duration.ofSeconds(1),Duration.ofSeconds(1),1));
        assertThrows(IllegalArgumentException.class,()->new GrpcKvClient(List.of(),transport,Duration.ofSeconds(1),Duration.ofSeconds(1),1));
        assertThrows(IllegalArgumentException.class,()->client(transport,Duration.ZERO,Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,()->client(transport,Duration.ofSeconds(1),Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class,()->client(transport,Duration.ofSeconds(Long.MAX_VALUE),Duration.ofSeconds(1)));
    }

    private static GrpcKvClient client(Stub transport,Duration budget,Duration cap) {
        return new GrpcKvClient(List.of(A,B),transport,budget,cap,17);
    }
    private static KvRpc.ClientCommandResponse ok(String value) {
        return KvRpc.ClientCommandResponse.ok(CommandResult.ok(value),"n1",A);
    }
    @FunctionalInterface interface Script {
        KvRpc.ClientCommandResponse call(int attempt,Command command,Duration timeout) throws IOException,InterruptedException;
    }
    private static final class Stub implements RaftTransport {
        final Script script;final List<Command> commands=new ArrayList<>();final List<Duration> timeouts=new ArrayList<>();
        final List<String> urls=new ArrayList<>();int closes;
        Stub(Script script){this.script=script;}
        @Override public <T>T postObject(String url,String path,Serializable request,Class<T> type,Duration timeout)
                throws IOException,InterruptedException {
            assertEquals("/client",path);assertTrue(timeout.toNanos()>0);
            Command command=((KvRpc.ClientCommandRequest)request).command();
            commands.add(command);timeouts.add(timeout);urls.add(url);
            return type.cast(script.call(commands.size(),command,timeout));
        }
        @Override public void close(){closes++;}
    }
}
