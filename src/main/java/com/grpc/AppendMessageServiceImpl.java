package com.grpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

  public String append(LogMessage msg) {

    Callable<LogMessageAck> callableTask_1 = () -> appendMsg("secondary-grpc", 9093, msg);

    Callable<LogMessageAck> callableTask_2 = () -> appendMsg("secondary-grpc-second", 9094, msg);;

    List<Callable<LogMessageAck>> callableTasks = new ArrayList<>();
    callableTasks.add(callableTask_1);
    callableTasks.add(callableTask_2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    List<Future<LogMessageAck>> futures = null;
    try {
      futures = executor.invokeAll(callableTasks);
    } catch (InterruptedException ex) {
      // TODO add logic
    }
    LogMessageAck generalResponse;
    LogMessageAck r1 = null;
    LogMessageAck r2 = null;
    try {
    r1 = futures.get(0).get(250000, TimeUnit.MILLISECONDS);
    r2 = futures.get(0).get(250000, TimeUnit.MILLISECONDS);
    } catch(Exception ex) {
      // TODO add logic
    }
    if(r1.getStatus().equals("OK") && r2.getStatus().equals("OK")) {
      generalResponse = LogMessageAck.newBuilder().setStatus("OK").build();
    } else {
      generalResponse = LogMessageAck.newBuilder().setStatus("Messages weren't replicated").setId(1L).build();
    }

    return generalResponse.getStatus();
  }

  private LogMessageAck appendMsg(String host, int port, LogMessage msg) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                    .usePlaintext()
                                                    .build();
    AppendMessageServiceGrpc.AppendMessageServiceBlockingStub stub = AppendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck response = stub.append(msg);
    channel.shutdown();
    try {
      channel.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      // TODO add logic
    }
    return response;
  }
}
