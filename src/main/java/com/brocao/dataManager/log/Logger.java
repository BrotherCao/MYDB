package com.brocao.dataManager.log;

import com.brocao.utils.Panic;
import com.brocao.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {

    void log(byte[] data);

    void truncate(long x) throws Exception;

    byte[] next();

    void rewind();

    void close();

    public static Logger create(String path) {
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(new Exception("log file already exists!"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(new Exception("file cannot rw"));
        }

        FileChannel fileChannel = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fileChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));

        try {
            fileChannel.position(0);
            fileChannel.write(buf);
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf,fileChannel,0);
    }

    public static Logger open(String path) {
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        if (!file.exists()) {
            Panic.panic(new Exception("file is not exists!"));
        }
        if (!file.canWrite() || !file.canRead()) {
            Panic.panic(new Exception("file cannot rw!"));
        }

        FileChannel fileChannel = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            fileChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        LoggerImpl lg = new LoggerImpl(raf, fileChannel);
        lg.init();
        return lg;

    }
}
