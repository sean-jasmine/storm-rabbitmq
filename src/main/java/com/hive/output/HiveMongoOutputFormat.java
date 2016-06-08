package main.java.com.hive.output;

import com.mongodb.DBCollection;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.output.MongoOutputCommitter;
import com.mongodb.hadoop.mapred.output.MongoRecordWriter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/*
 * Define a HiveMongoOutputFormat that specifies how Hive should write data in
 * Hive tables into MongoDB
 */
public class HiveMongoOutputFormat implements HiveOutputFormat<BSONWritable, BSONWritable> {

    public RecordWriter getHiveRecordWriter(final JobConf conf,
                                            final Path finalOutPath,
                                            final Class<? extends Writable> valueClass,
                                            final boolean isCompressed,
                                            final Properties tableProperties,
                                            final Progressable progress) throws IOException {
        return new HiveMongoRecordWriter(MongoConfigUtil.getOutputCollections(conf), conf);
    }


    public void checkOutputSpecs(final FileSystem arg0, final JobConf arg1) throws IOException {
    }


    public org.apache.hadoop.mapred.RecordWriter<BSONWritable, BSONWritable>
    getRecordWriter(final FileSystem arg0, final JobConf arg1, final String arg2, final Progressable arg3) throws IOException {
        throw new IOException("Hive should call 'getHiveRecordWriter' instead of 'getRecordWriter'");
    }


    /*
     * HiveMongoRecordWriter ->
     * MongoRecordWriter used to write from Hive into BSON Objects
     */
    private class HiveMongoRecordWriter
        extends MongoRecordWriter<Object, BSONWritable>
        implements RecordWriter {

        private final TaskAttemptContext context;
        private final MongoOutputCommitter committer;

        public HiveMongoRecordWriter(final List<DBCollection> ls, final JobConf conf) {
            super(conf);
            context = new TaskAttemptContextImpl(
              conf, TaskAttemptID.forName(conf.get("mapred.task.id")));
            // Hive doesn't use an OutputCommitter, so we'll need to do this
            // manually.
            committer = new MongoOutputCommitter(ls);
        }

        public void close(final boolean abort) throws IOException {
            // Disambiguate call to super.close().
            super.close((TaskAttemptContext) null);
            if (abort) {
                committer.abortTask(context);
            } else if (committer.needsTaskCommit(context)) {
                committer.commitTask(context);
            }
        }

        public void write(final Writable w) throws IOException {
            super.write(null, (BSONWritable) w);
        }
    }
}
