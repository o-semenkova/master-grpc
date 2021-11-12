package com.grpc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Service;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

  public LogMessageAck append(LogMessage msg) {
    CountDownLatch latch = new CountDownLatch(msg.getW() - 1);
      appendMsg("secondary-grpc", 9093, msg, latch);
      appendMsg("secondary-grpc-second", 9094, msg, latch);
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return LogMessageAck.newBuilder().setStatus("OK").build();
  }

  private void appendMsg(String host, int port, LogMessage msg, CountDownLatch latch) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                    .usePlaintext()
                                                    .build();
    AppendMessageServiceGrpc.AppendMessageServiceStub stub = AppendMessageServiceGrpc.newStub(channel);
    stub.append(msg, new LogMessageCallback(latch));
    try {
      channel.awaitTermination(5000, TimeUnit.MILLISECONDS);
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    channel.shutdownNow();
  }
}
