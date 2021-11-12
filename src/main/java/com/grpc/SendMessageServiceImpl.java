package com.grpc;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class SendMessageServiceImpl extends SendMessageServiceGrpc.SendMessageServiceImplBase {

  private ConcurrentNavigableMap<Long, String> messages = new ConcurrentSkipListMap<>();
  private AppendMessageServiceImpl appendMessageService;
  private Long counter = 1L;

  public SendMessageServiceImpl(AppendMessageServiceImpl appendMessageService) {
    this.appendMessageService = appendMessageService;
  }

  public void send(LogMessage request, StreamObserver<LogMessageAck> responseObserver) {
    Long internalId = counter++;
    LogMessage msgToReplicate = LogMessage.newBuilder()
                               .setId(internalId)
                               .setW(request.getW())
                               .setMsg(request.getMsg())
                               .build();
    messages.put(internalId, "write_concern=" + request.getW() + ", msg= " + request.getMsg());
    LogMessageAck response = appendMessageService.append(msgToReplicate);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private String convertWithStream(Map<Long, String> map) {
    String mapAsString = map.keySet().stream()
                            .map(key -> map.get(key))
                            .collect(Collectors.joining(", ", "{", "}"));
    return mapAsString;
  }

  public String getAllMessages() {
    return convertWithStream(messages);
  }
}
