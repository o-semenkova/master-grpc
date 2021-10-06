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
    AppendMessageServiceGrpc.AppendMessageServiceBlockingStub stub_1 = AppendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck response = stub_1.append(msg);
    channel.shutdownNow();

    return response;
  }
}
