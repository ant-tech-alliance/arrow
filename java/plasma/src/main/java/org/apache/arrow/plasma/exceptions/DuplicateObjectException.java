package org.apache.arrow.plasma.exceptions;

public class DuplicateObjectException extends Exception {

  public DuplicateObjectException (String msg) {
    super(msg);
  }

  public DuplicateObjectException (String msg, Throwable t) {
    super(msg, t);
  }
}
