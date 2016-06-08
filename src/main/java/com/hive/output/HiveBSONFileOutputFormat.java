package main.java.com.hive.output;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.BSONFileOutputFormat;
import com.mongodb.hadoop.mapred.output.BSONFileRecordWriter;
import com.mongodb.hadoop.splitter.BSONSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * An OutputFormat that writes BSON files
 */
@SuppressWarnings("deprecation")
public class HiveBSONFileOutputFormat<K, V>
    extends BSONFileOutputFormat<K, V> implements HiveOutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(HiveBSONFileOutputFormat.class);
    public static final String MONGO_OUTPUT_FILE = "mongo.output.file";

    /**
     * create the final output file
     *
     * @param jc              the job configuration
     * @param fileOutputPath  the file that the output should be directed at
     * @param valueClass      the value class used to create
     * @param tableProperties the tableInfo for this file's corresponding table
     * @return RecordWriter for the output file
     */
    public RecordWriter getHiveRecordWriter(final JobConf jc,
                                            final Path fileOutputPath,
                                            final Class<? extends Writable> valueClass,
                                            final boolean isCompressed,
                                            final Properties tableProperties,
                                            final Progressable progress) throws IOException {

        LOG.info("Output going into " + fileOutputPath);

        FileSystem fs = fileOutputPath.getFileSystem(jc);
        FSDataOutputStream outFile = fs.create(fileOutputPath);

        FSDataOutputStream splitFile = null;
        if (MongoConfigUtil.getBSONOutputBuildSplits(jc)) {
            Path splitPath = new Path(fileOutputPath.getParent(), "." + fileOutputPath.getName() + ".splits");
            splitFile = fs.create(splitPath);
        }

        long splitSize = BSONSplitter.getSplitSize(jc, null);

        return new HiveBSONFileRecordWriter(outFile, splitFile, splitSize);
    }


    /**
     * A Hive Record Write that calls the BSON one
     */
    public class HiveBSONFileRecordWriter<K, V>
        extends BSONFileRecordWriter<K, V>
        implements RecordWriter {

        public HiveBSONFileRecordWriter(final FSDataOutputStream outFile, final FSDataOutputStream splitFile, final long splitSize) {
            super(outFile, splitFile, splitSize);
        }

        public void close(final boolean toClose) throws IOException {
            super.close((TaskAttemptContext) null);
        }

        public void write(final Writable value) throws IOException {
            super.write(null, (BSONWritable) value);
        }
    }
}
