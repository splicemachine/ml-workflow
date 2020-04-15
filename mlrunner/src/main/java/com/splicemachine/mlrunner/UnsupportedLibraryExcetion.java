package com.splicemachine.mlrunner;

public class UnsupportedLibraryExcetion extends Exception { 
    /**
     * Splice Machine Exception for model library types that are unsupported
     */
    private static final long serialVersionUID = -5505394720858459276L;

    public UnsupportedLibraryExcetion(String errorMessage) {
        super(errorMessage);
    }
}