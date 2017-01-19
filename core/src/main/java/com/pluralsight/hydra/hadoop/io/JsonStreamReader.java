package com.pluralsight.hydra.hadoop.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * The JsonStreamReader handles byte-by-byte reading of a JSON stream, creating
 * records based on a base 'identifier'. This identifier is given at object
 * creation.
 */
public class JsonStreamReader extends BufferedReader {

    private StringBuilder bldr = new StringBuilder();

    private String identifier = null;

    private long bytesRead = 0;

    public JsonStreamReader(String identifier, InputStream strm) {
        super(new InputStreamReader(strm));
        this.identifier = identifier;
    }

    /**
     * Advances the input stream to the next JSON record, returned a String
     * object.
     *
     * @return A string of JSON or null
     * @throws IOException If an error occurs reading from the stream
     */
    public String getJsonRecord() throws IOException {
        bldr.delete(0, bldr.length());

        boolean foundRecord = false;

        int c = 0, numBraces = 1;
        while ((c = super.read()) != -1) {
            ++bytesRead;
            if (!foundRecord) {
                bldr.append((char) c);

                if (bldr.toString().contains(identifier)) {
                    forwardToBrace();
                    foundRecord = true;

                    bldr.delete(0, bldr.length());
                    bldr.append('{');
                }
            } else {
                bldr.append((char) c);

                if (c == '{') {
                    ++numBraces;
                } else if (c == '}') {
                    --numBraces;
                }

                if (numBraces == 0) {
                    break;
                }
            }
        }

        if (foundRecord) {
            return bldr.toString();
        } else {
            return null;
        }
    }

    /**
     * Gets the number of bytes read by the stream reader
     *
     * @return The number of bytes read
     */
    public long getBytesRead() {
        return bytesRead;
    }

    private void forwardToBrace() throws IOException {
        while (super.read() != '{') {
        }
    }
}