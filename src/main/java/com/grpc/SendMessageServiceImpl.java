package com.grpc;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class SendMessageServiceImpl extends SendMessageServiceGrpc.SendMessageServiceImplBase {

  private HashMap<Long, String> messages = new HashMap<>();
  private AppendMessageServiceImpl appendMessageService;

  public SendMessageServiceImpl(AppendMessageServiceImpl appendMessageService) {
    this.appendMessageService = appendMessageService;
  }

  public void send(LogMessage request, StreamObserver<LogMessageAck> responseObserver) {
    LogMessage msg = LogMessage.newBuilder().setId(request.getId()).setText(request.getText()).build();
    messages.put(request.getId(), request.getText());
    LogMessageAck ack1 = appendMessageService.append(msg);
    responseObserver.onNext(ack1);
    responseObserver.onCompleted();
  }

  private String convertWithStream(Map<Long, String> map) {
    String mapAsString = map.keySet().stream()
                            .map(key -> key + "=" + map.get(key))
                            .collect(Collectors.joining(", ", "{", "}"));
    return mapAsString;
  }

  public String getAllMessages() {
    return convertWithStream(messages);
  }
}
