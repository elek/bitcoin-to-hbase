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
import org.zuinnote.hadoop.bitcoin.format.BitcoinTransaction;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static org.zuinnote.hadoop.bitcoin.format.BitcoinUtil.*;

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

        HTable blockTable = new HTable(conf, "block");
        HTable transactionTable = new HTable(conf, "transaction");
        blockTable.setAutoFlush(false);
        transactionTable.setAutoFlush(false);

        Files.list(blockDir).filter(path -> path.getFileName().toString().startsWith("blk")).forEach(p -> {
            try {
                load(p.toFile(), blockTable, transactionTable);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });


        Thread.sleep(1000);
    }

    private void load(File file, HTable blockTable, HTable transactionTable) throws Exception {

        System.out.println("Processing file: " + file);

        BitcoinBlockReader reader = new BitcoinBlockReader(new FileInputStream(file), DEFAULT_MAXSIZE_BITCOINBLOCK, DEFAULT_BUFFERSIZE, DEFAULT_MAGIC, true);
        BitcoinBlock block;
        int record = 0;
        try {
            while ((block = reader.readBlock()) != null) {
                byte[] reverseBlockHash = Bytes.toBytes(convertByteArrayToHexString(reverseByteArray(getBlockHash(block))));
                loadBlocks(file, reverseBlockHash, block, blockTable);
                loadTransactions(file, reverseBlockHash, block, transactionTable);

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void loadTransactions(File file, byte[] reverseBlockHash, BitcoinBlock block, HTable transactionTable) {
        try {
            List<Put> puts = new ArrayList<>();

            puts.addAll(transactiontsToPut(file, reverseBlockHash, block));
            if (!dryRun) {
                transactionTable.put(puts);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void loadBlocks(File file, byte[] reverseBlockHash, BitcoinBlock block, HTable blockTable) {
        try {
            List<Put> puts = new ArrayList<>();

            puts.add(blockToPut(file, reverseBlockHash, block));
            if (!dryRun) {
                blockTable.put(puts);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private List<Put> transactiontsToPut(File file, byte[] reverseBlockHash, BitcoinBlock block) throws IOException, NoSuchAlgorithmException {
        List<Put> result = new ArrayList<>();
        for (BitcoinTransaction tr : block.getTransactions()) {
            byte[] transactionHash = reverseByteArray(getTransactionHash(tr));
            Put p = new Put(Bytes.toBytes(convertByteArrayToHexString(reverseByteArray(transactionHash))));
            p.setWriteToWAL(false);
            p.add(Bytes.toBytes("t"), Bytes.toBytes("is"), Bytes.toBytes(tr.getListOfInputs().size()));
            p.add(Bytes.toBytes("t"), Bytes.toBytes("os"), Bytes.toBytes(tr.getListOfOutputs().size()));
            p.add(Bytes.toBytes("t"), Bytes.toBytes("tc"), Bytes.toBytes(block.getTransactionCounter()));
            p.add(Bytes.toBytes("t"), Bytes.toBytes("b"), Bytes.toBytes(convertByteArrayToHexString(reverseByteArray(reverseBlockHash))));
            result.add(p);
        }
        return result;
    }

    private Put blockToPut(File file, byte[] reverseBlockHash, BitcoinBlock block) throws NoSuchAlgorithmException, IOException {
        Put p = new Put(reverseBlockHash);
        p.setWriteToWAL(false);
        p.add(Bytes.toBytes("b"), Bytes.toBytes("d"), Bytes.toBytes(block.getTime()));
        p.add(Bytes.toBytes("b"), Bytes.toBytes("s"), Bytes.toBytes(block.getBlockSize()));
        p.add(Bytes.toBytes("b"), Bytes.toBytes("t"), Bytes.toBytes(block.getTransactionCounter()));
        p.add(Bytes.toBytes("b"), Bytes.toBytes("f"), Bytes.toBytes(file.getName().toString()));
        return p;
    }
}
