package com.grpc;

import java.util.concurrent.CountDownLatch;
import io.grpc.stub.StreamObserver;

public class LogMessageCallback implements StreamObserver<LogMessageAck> {

  private final CountDownLatch countDownLatch;

  public LogMessageCallback(CountDownLatch latch) {
    this.countDownLatch = latch;
  }

  @Override
  public void onNext(LogMessageAck logMessageAck) {
    countDownLatch.countDown();
  }

  @Override
  public void onError(Throwable throwable) {

    countDownLatch.countDown();
  }

  @Override
  public void onCompleted() {

  }
}
