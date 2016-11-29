package net.anzix.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.zuinnote.hadoop.bitcoin.format.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.BitcoinBlockReader;
import org.zuinnote.hadoop.bitcoin.format.BitcoinUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Bitcoin transaction/block importer to hbase.
 */
public class BitcoinImport {

    static final int DEFAULT_BUFFERSIZE = 64 * 1024;

    static final int DEFAULT_MAXSIZE_BITCOINBLOCK = 1 * 1024 * 1024;

    static final byte[][] DEFAULT_MAGIC = {{(byte) 0xF9, (byte) 0xBE, (byte) 0xB4, (byte) 0xD9}};

    @Option(name = "-dir", usage = "Dir which contains blockhain block files", required = true)
    private Path blockDir;

    @Option(name = "-zk", usage = "HBase Zookeeper quorum address", required = true)
    private String quorum;

    @Option(name = "-n", usage = "Dry run")
    private boolean dryRun = false;

    public static void main(String[] args) throws Exception {
        BitcoinImport bitcoinImport = new BitcoinImport();
        CmdLineParser parser = new CmdLineParser(bitcoinImport);
        try {
            parser.parseArgument(args);
            bitcoinImport.run();
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }

    private void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);

        HTable hTable = new HTable(conf, "block");
        hTable.setAutoFlush(false);

        Files.list(blockDir).filter(path -> path.getFileName().toString().startsWith("blk")).forEach(p -> {
            try {
                load(p.toFile(), hTable);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });


        Thread.sleep(1000);
    }

    private void load(File file, HTable htable) throws Exception {

        System.out.println("Processing file: " + file);
        List<Put> puts = new ArrayList<>();

        BitcoinBlockReader reader = new BitcoinBlockReader(new FileInputStream(file), DEFAULT_MAXSIZE_BITCOINBLOCK, DEFAULT_BUFFERSIZE, DEFAULT_MAGIC, true);
        BitcoinBlock block;
        int record = 0;
        try {
            while ((block = reader.readBlock()) != null) {
                Put p = blockToPut(file, block);
                puts.add(p);
                record++;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        if (!dryRun) {
            htable.put(puts);
            htable.flushCommits();
        }
        System.out.println("Imported records: " + record);


    }

    private Put blockToPut(File file, BitcoinBlock block) throws NoSuchAlgorithmException, IOException {
        Put p = new Put(Bytes.toBytes(BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(BitcoinUtil.getBlockHash(block)))));
        p.setWriteToWAL(false);
        p.add(Bytes.toBytes("b"), Bytes.toBytes("d"), Bytes.toBytes(block.getTime()));
        p.add(Bytes.toBytes("b"), Bytes.toBytes("s"), Bytes.toBytes(block.getBlockSize()));
        p.add(Bytes.toBytes("b"), Bytes.toBytes("t"), Bytes.toBytes(block.getTransactionCounter()));
        p.add(Bytes.toBytes("b"), Bytes.toBytes("f"), Bytes.toBytes(file.getName().toString()));
        return p;
    }
}
