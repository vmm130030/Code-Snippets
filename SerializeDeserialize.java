import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class SerializeDeserialize {

	public static byte[] SerializeMsg(Object obj) throws IOException {
        ObjectOutputStream out;// = new ObjectOutputStream();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        out = new ObjectOutputStream(outputStream);
        out.writeObject(obj);
        return outputStream.toByteArray();
    }

    public static Object DeserializeMsg(byte[] bytes) throws IOException, ClassNotFoundException {
        ObjectInputStream inputStream;// = new ObjectOutputStream();
        ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
        inputStream = new ObjectInputStream(bos);
        return inputStream.readObject();
    }	
}
