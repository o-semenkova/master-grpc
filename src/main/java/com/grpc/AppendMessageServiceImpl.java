package com.grpc;

import org.springframework.stereotype.Service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

  public LogMessageAck append(LogMessage msg) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9093)
                                                  .usePlaintext()
                                                  .build();
    AppendMessageServiceGrpc.AppendMessageServiceBlockingStub stub = AppendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck response = stub.append(msg);
    channel.shutdownNow();

    return response;
  }
}
