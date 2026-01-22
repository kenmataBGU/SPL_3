package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class StompMessageEncoderDecoder implements MessageEncoderDecoder<String> {

    private byte[] bytes = new byte[1 << 10]; // start with 1k buffer
    private int len = 0;

    @Override
    public String decodeNextByte(byte nextByte) {
        // The Frame delimiter in STOMP is the null char
        if (nextByte == '\0') {
            return popString();
        }

        pushByte(nextByte);
        return null; // Frame not complete
    }

    @Override
    public byte[] encode(String message) {
        // FIX: Do NOT append "\0" here. 
        // The Protocol's Frame.toString() already appends the null terminator.
        return message.getBytes(StandardCharsets.UTF_8);
    }

    private void pushByte(byte nextByte) {
        if (len >= bytes.length) {
            bytes = Arrays.copyOf(bytes, len * 2);
        }
        bytes[len++] = nextByte;
    }

    private String popString() {
        String result = new String(bytes, 0, len, StandardCharsets.UTF_8);
        len = 0;
        return result;
    }
}