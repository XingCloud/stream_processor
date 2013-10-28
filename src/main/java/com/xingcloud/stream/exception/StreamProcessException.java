package com.xingcloud.stream.exception;

/**
 * User: Z J Wu Date: 13-10-23 Time: 上午10:15 Package: com.xingcloud.storm.exception
 */
public class StreamProcessException extends Exception {

  public StreamProcessException() {
  }

  public StreamProcessException(String message) {
    super(message);
  }

  public StreamProcessException(String message, Throwable cause) {
    super(message, cause);
  }

  public StreamProcessException(Throwable cause) {
    super(cause);
  }
}
