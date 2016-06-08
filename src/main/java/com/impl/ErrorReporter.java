package main.java.com.impl;

import java.io.Serializable;

public interface ErrorReporter extends Serializable {
    void reportError(Throwable t);
}
