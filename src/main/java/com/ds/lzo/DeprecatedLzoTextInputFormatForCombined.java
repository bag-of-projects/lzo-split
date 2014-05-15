package com.ds.lzo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.hadoop.compression.lzo.LzoInputFormatCommon;
import com.hadoop.mapred.DeprecatedLzoTextInputFormat;

public class DeprecatedLzoTextInputFormatForCombined extends DeprecatedLzoTextInputFormat {
    @SuppressWarnings("unchecked")
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
        JobConf conf, Reporter reporter) throws IOException {
        LOG.warn("ENTERING GETRECORDREADER");
      FileSplit fileSplit = (FileSplit) split;
      LOG.warn("split type: " + split.getClass());
      if (LzoInputFormatCommon.isLzoFile(fileSplit.getPath().toString())) {
        reporter.setStatus(fileSplit.toString());
        return new DeprecatedLzoLineRecordReaderForCombined(conf, fileSplit);
      } else {
        // ignore non-LZO files.
          return new DummyRecordReader();
      }
    }

    public class DummyRecordReader implements RecordReader<LongWritable, Text> {

        @Override
        public void close() throws IOException {

        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public float getProgress() throws IOException {
            return 0;
        }

        @Override
        public boolean next(LongWritable arg0, Text arg1) throws IOException {
            return false;
        }

    }

}
