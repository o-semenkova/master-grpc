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

  public LogMessageAck append(LogMessage msg) {

    Callable<LogMessageAck> callableTask_1 = () -> appendMsg(9093, msg);

    Callable<LogMessageAck> callableTask_2 = () -> appendMsg(9094, msg);

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
    r1 = futures.get(0).get(300, TimeUnit.MILLISECONDS);
    r2 = futures.get(0).get(300, TimeUnit.MILLISECONDS);
    } catch(Exception ex) {
      // TODO add logic
    }
    if(r1.getStatus().equals("OK") && r2.getStatus().equals("OK")) {
      generalResponse = LogMessageAck.newBuilder().setStatus("OK").setId(1L).build();
    } else {
      generalResponse = LogMessageAck.newBuilder().setStatus("FALSE").setId(2L).build();
    }
    return generalResponse;
  }

  private LogMessageAck appendMsg(int port, LogMessage msg) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                                                    .usePlaintext()
                                                    .build();
    AppendMessageServiceGrpc.AppendMessageServiceBlockingStub stub = AppendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck response = stub.append(msg);
    channel.shutdownNow();
    return response;
  }
}
