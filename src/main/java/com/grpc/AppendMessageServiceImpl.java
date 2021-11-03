package com.grpc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

  public String append(LogMessage msg, CountDownLatch latch) {

    int writeConcern = msg.getW();
    if(writeConcern == 1) {
      appendMsg("secondary-grpc", 9093, msg);
      appendMsg("secondary-grpc-second", 9094, msg);
    } else if (writeConcern == 2) {
      appendMsg("secondary-grpc", 9093, msg, latch);
      appendMsg("secondary-grpc-second", 9094, msg);
    } else {
      appendMsg("secondary-grpc", 9093, msg, latch);
      appendMsg("secondary-grpc-second", 9094, msg, latch);
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return LogMessageAck.newBuilder().setStatus("OK").build().getStatus();
  }

  private void appendMsg(String host, int port, LogMessage msg, CountDownLatch latch) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                    .usePlaintext()
                                                    .build();
    AppendMessageServiceGrpc.AppendMessageServiceStub stub = AppendMessageServiceGrpc.newStub(channel);
    stub.append(msg, new LogMessageCallback());

    try {
      channel.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      // TODO add logic
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    channel.shutdown();
    latch.countDown();
  }

  private void appendMsg(String host, int port, LogMessage msg) {

    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                    .usePlaintext()
                                                    .build();
      AppendMessageServiceGrpc.AppendMessageServiceStub stub = AppendMessageServiceGrpc.newStub(channel);
      stub.append(msg, new LogMessageCallback());

      try {
        channel.awaitTermination(5000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ex) {
        // TODO add logic
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      channel.shutdown();
  }
}
