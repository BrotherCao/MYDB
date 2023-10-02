package tm;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileTest {

    @Test
    public void fileTest() {
        ByteBuffer buf = ByteBuffer.wrap(new byte[4]);
        File file = new File("testfile");
        RandomAccessFile rw = null;
        FileChannel fileChannel = null;
        try {
            rw = new RandomAccessFile(file, "rw");
            fileChannel = rw.getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        String s = "x";
        ByteBuffer wrap = ByteBuffer.wrap(s.getBytes());
        try {
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(new String(buf.array()));
    }

    @Test
    public void fileTest2() {

    }
}
