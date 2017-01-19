package com.pluralsight.hydra.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;

/**
 * The JsonInputFormat will read two types of JSON formatted data. The default
 * expectation is each JSON record is newline delimited. This method is
 * generally faster and is backed by the {@link LineRecordReader} you are likely
 * familiar with. The other method is 'pretty print' of JSON records, where
 * records span multiple lines and often have some type of root identifier. This
 * method is likely slower, but respects record boundaries much like the
 * LineRecordReader.<br>
 * <br>
 * Use of the 'pretty print' reader requires a record identifier.
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {

    private static JsonFactory factory = new JsonFactory();

    private static ObjectMapper mapper = new ObjectMapper(factory);

    public static final String ONE_RECORD_PER_LINE = "json.input.format.one.record.per.line";

    public static final String RECORD_IDENTIFIER = "json.input.format.record.identifier";

    @Override
    public RecordReader<LongWritable, Text>  createRecordReader(InputSplit split, TaskAttemptContext context) throws
            IOException, InterruptedException {
        RecordReader<LongWritable, Text>  rdr;
        if (context.getConfiguration().getBoolean(ONE_RECORD_PER_LINE, true)) {
            rdr = new SimpleJsonRecordReader();
        } else {
            return new JsonRecordReader();
        }
        rdr.initialize(split, context);
        return rdr;
    }

    /**
     * This class uses the {@link LineRecordReader} to read a line of JSON and
     * return it as a Text object.
     */
    public static class SimpleJsonRecordReader extends RecordReader<LongWritable, Text>  {

        private LineRecordReader rdr = null;

        private LongWritable outkey = new LongWritable(0l);

        private Text outvalue = new Text();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            rdr = new LineRecordReader();
            rdr.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (rdr.nextKeyValue()) {
                outvalue.set(rdr.getCurrentValue());
                outkey.set(rdr.getCurrentKey().get());
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            rdr.close();
        }

        @Override
        public float getProgress() throws IOException {
            return rdr.getProgress();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return outkey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return outvalue;
        }
    }

    /**
     * This class uses the {@link JsonStreamReader} to read JSON records from a
     * file. It respects split boundaries to complete full JSON records, as
     * specified by the root identifier. This class will discard any records
     * that it was unable to decode using
     * {@link JsonInputFormat#decodeLineToJsonNode(String)}
     */
    public static class JsonRecordReader extends RecordReader<LongWritable, Text>  {

        private Logger LOG = Logger.getLogger(JsonRecordReader.class);

        private JsonStreamReader rdr = null;

        private long start = 0, end = 0;

        private float toRead = 0;

        private String identifier = null;

        private Logger log = Logger.getLogger(JsonRecordReader.class);

        private Text outJson = new Text();

        private LongWritable outKey = null;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            this.identifier = context.getConfiguration().get(RECORD_IDENTIFIER);

            if (this.identifier == null || identifier.isEmpty()) {
                throw new InvalidParameterException(JsonInputFormat.RECORD_IDENTIFIER + " is not set.");
            } else {
                LOG.info("Initializing JsonRecordReader with identifier " + identifier);
            }

            FileSplit fSplit = (FileSplit) split;

            // get relevant data
            Path file = fSplit.getPath();

            log.info("File is " + file);

            start = fSplit.getStart();
            end = start + split.getLength();
            toRead = end - start;

            FSDataInputStream strm = FileSystem.get(context.getConfiguration()).open(file);

            if (start != 0) {
                strm.seek(start);
            }

            rdr = new JsonStreamReader(identifier, new BufferedInputStream(strm));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean retval = false;
            boolean keepGoing = false;
            do {
                keepGoing = false;
                String record = rdr.getJsonRecord();
                if (record != null) {
                    if (JsonInputFormat.decodeLineToJsonNode(record) == null) {
                        keepGoing = true;
                    } else {
                        outJson.set(record);
                        outKey.set(rdr.getBytesRead());
                        retval = true;
                    }
                }
            } while (keepGoing);

            return retval;
        }

        @Override
        public void close() throws IOException {
            rdr.close();
        }

        @Override
        public float getProgress() throws IOException {
            return (float) rdr.getBytesRead() / toRead;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return outKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return outJson;
        }
    }

    /**
     * Decodes a given string of text to a {@link JsonNode}.
     *
     * @param line The line of text
     * @return The JsonNode or null if a JsonParseException,
     * JsonMappingException, or IOException error occurs
     */
    public static synchronized JsonNode decodeLineToJsonNode(String line) {

        try {
            return mapper.readTree(line);
        } catch (JsonParseException e) {
            e.printStackTrace();
            return null;
        } catch (JsonMappingException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Sets the input format to use the {@link SimpleJsonRecordReader} if true,
     * otherwise {@link JsonRecordReader}.<br>
     * <br>
     * Default is true.
     *
     * @param job                The job to configure
     * @param isOneRecordPerLine True if JSON records are new line delimited, false otherwise.
     */
    public static void setOneRecordPerLine(Job job, boolean isOneRecordPerLine) {
        job.getConfiguration().setBoolean(ONE_RECORD_PER_LINE, isOneRecordPerLine);
    }

    /**
     * Gets if this is configured as one JSON record per line.
     *
     * @param conf the Job configuration
     * @return True if one JSON record per line, false otherwise.
     */
    public static boolean getOneRecordPerLine(Configuration conf) {
        return conf.getBoolean(ONE_RECORD_PER_LINE, true);
    }

    /**
     * Specifies a record identifier to be used with the
     * {@link JsonRecordReader}<br>
     * <br>
     * Must be set if {@link JsonInputFormat#setOneRecordPerLine} is false.
     *
     * @param job    The job to configure
     * @param record The record identifier
     */
    public static void setRecordIdentifier(Job job, String record) {
        job.getConfiguration().set(RECORD_IDENTIFIER, record);
    }

    /**
     * Gets the record identifier
     *
     * @param conf the Job configuration
     * @return The record identifier or null if not set
     */
    public static String getRecordIdentifier(Configuration conf) {
        return conf.get(RECORD_IDENTIFIER);
    }
}

