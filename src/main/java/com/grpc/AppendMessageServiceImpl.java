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

    Callable<LogMessageAck> callableTask_1 = () -> appendMsg(9093, msg);

    Callable<LogMessageAck> callableTask_2 = () -> {
      ManagedChannel channel = ManagedChannelBuilder.forAddress("secondary-grpc-second", 9094)
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
    };

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

    while(!r1.getStatus().equals("OK") && !r2.getStatus().equals("OK")) {
      System.out.println("Waiting for all answers...");
    }
    generalResponse = LogMessageAck.newBuilder().setStatus("OK").setId(1L).build();

    return generalResponse.getStatus();
  }

  private LogMessageAck appendMsg(int port, LogMessage msg) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("secondary-grpc", port)
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
