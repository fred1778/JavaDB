package edu.uob;

import java.io.IOException;
import java.io.Serial;

public class DBException extends IOException {
    @Serial
    private static final long serialVersionUID = 1;
    public DBException(String message) {
        super(message);
    }

}

