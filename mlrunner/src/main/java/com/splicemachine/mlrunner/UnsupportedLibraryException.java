package com.splicemachine.mlrunner;

import java.sql.SQLException;

public class UnsupportedLibraryException extends SQLException {
    /**
     * Splice Machine Exception for model library types that are unsupported
     */
    private static final long serialVersionUID = -5505394720858459276L;

    public UnsupportedLibraryException(String errorMessage) {
        super(errorMessage);
    }
}