package com.xingcloud.stream.exception;

/**
 * User: Z J Wu Date: 13-10-23 Time: 上午10:15 Package: com.xingcloud.storm.exception
 */
public class StreamProcessRuntimeException extends RuntimeException {

  public StreamProcessRuntimeException() {
  }

  public StreamProcessRuntimeException(String message) {
    super(message);
  }

  public StreamProcessRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public StreamProcessRuntimeException(Throwable cause) {
    super(cause);
  }
}
