package raftkv.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public final class BinaryCodec {
    private BinaryCodec() {
    }

    public static byte[] encode(Serializable value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }

    public static Object decode(byte[] data) throws IOException {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
            try {
                return in.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Unknown serialized type", e);
            }
        }
    }
}

