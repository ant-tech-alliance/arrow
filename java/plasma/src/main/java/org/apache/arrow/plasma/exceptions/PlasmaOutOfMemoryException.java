package org.apache.arrow.plasma.exceptions;

public class PlasmaOutOfMemoryException extends Exception {

  public PlasmaOutOfMemoryException (String msg) {
    super(msg);
  }

  public PlasmaOutOfMemoryException (String msg, Throwable t) {
    super(msg, t);
  }

}
