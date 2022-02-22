package com.alec.rocksdb;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.rocksdb.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FileGenerator {
    public static void main(String[] args) {
        RocksDB.loadLibrary();

        String filePath = ".//src/test/rocks";
        try {
            buildRocksDb(filePath);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void generateSst(String filePath) throws RocksDBException, InterruptedException {
        SstFileWriter writer = new SstFileWriter(new EnvOptions(), new Options());
        writer.open(filePath + "/sstfile/" + "/00001.sst");
        for (long i=0; i<10000000;i++) {
            writer.put(String.format("%09d", i).getBytes(StandardCharsets.UTF_8),
                    (i + "_value").getBytes(StandardCharsets.UTF_8));
        }
        writer.finish();

        writer.open(filePath + "/sstfile/" + "/00002.sst");
        for (long i=10000000; i<20000000;i++) {
            writer.put(String.format("%09d", i).getBytes(StandardCharsets.UTF_8),
                    (i + "_value").getBytes(StandardCharsets.UTF_8));
        }
        writer.finish();

        writer.open(filePath + "/sstfile/" + "/00003.sst");
        for (long i=20000000; i<30000000;i++) {
            writer.put(String.format("%09d", i).getBytes(StandardCharsets.UTF_8),
                    (i + "_value").getBytes(StandardCharsets.UTF_8));
        }
        writer.finish();
        Thread.sleep(1000);
        writer.close();
    }

    public static void buildRocksDb(String filePath) throws RocksDBException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, filePath + "/untarfile/rocks");
        try {
            List<String> files = new ArrayList<>();
            files.add(filePath + "/untarfile/" + "/00001.sst");
            files.add(filePath + "/untarfile/" + "/00002.sst");
            files.add(filePath + "/untarfile/" + "/00003.sst");
            db.ingestExternalFile(files, new IngestExternalFileOptions());

            System.out.println(new String(db.get("020000000".getBytes())));
        } catch (Exception e) {
            System.out.println("failed:" + e.getMessage());
        } finally {
            db.close();
        }
    }

    public static void readRocksDb(String filePath) throws RocksDBException {
        Options options = new Options();
        RocksDB db = RocksDB.open(options, filePath);
//        Random random = new Random();
//        for (long i=0; i<1000;i++) {
//            String key = String.format("%09d", random.nextInt(30000000));
//            byte[] value = db.get(key.getBytes(StandardCharsets.UTF_8));
//            if (value == null) {
//                System.out.println("null value key:" + key);
//            } else {
//                System.out.println(new String(value));
//            }
//        }

        // 無key返回null   空字符串返回空的byte數組
//        db.put("empty1".getBytes(), "".getBytes());
//        db.put("empty2".getBytes(), " ".getBytes());
//        db.put("empty3".getBytes(), "   ".getBytes());
//        byte[] value;
//
//        value = db.get("empty1".getBytes());
//        String str = new String(value);
//        value = db.get("empty2".getBytes());
//        str = new String(value);
//        value = db.get("empty3".getBytes());
//        str = new String(value);
//        value = db.get("emptyNo".getBytes());
//        str = new String(value);

    }

    public static void fileDeTar(String filePath) throws IOException {
        InputStream in = new FileInputStream(new File(filePath + "/tarfile/files.tar"));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        int read;
        while ((read = in.read(buf)) > 0)
            baos.write(buf, 0, read);
        byte[] data = baos.toByteArray();
        SeekableByteArrayInputStream bais = new SeekableByteArrayInputStream(data);
        FSDataInputStream fsIn = new FSDataInputStream(bais);

        String untarFile = filePath + "/untarfile";
        TarArchiveInputStream tarIn = new TarArchiveInputStream(fsIn);
        TarArchiveEntry entry = null;
        while ((entry = tarIn.getNextTarEntry()) != null) {
            String dir = untarFile + "/" + entry.getName();
            File dirFile = new File(dir);

            if (entry.isDirectory()) {
                continue;
            } else {
                FileOutputStream bos = new FileOutputStream(dirFile);
                IOUtils.copyBytes(tarIn, bos, 1024);
            }

        }
    }
}
