package com.ds.lzo;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextOutputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapred.DeprecatedLzoTextInputFormat;


/**
 * Scheme for LZO encoded text files.
 */
@SuppressWarnings("deprecation")
public class LzoTextLine extends TextLine {

    public LzoTextLine() {
        super();
    }

    public LzoTextLine(int numSinkParts) {
        super(numSinkParts);
    }

    public LzoTextLine(Fields sourceFields, Fields sinkFields) {
        super(sourceFields, sinkFields);
    }

    public LzoTextLine(Fields sourceFields, Fields sinkFields, int numSinkParts) {
        super(sourceFields, sinkFields, numSinkParts);
    }

    public LzoTextLine(Fields sourceFields) {
        super(sourceFields);
    }

    public LzoTextLine(Fields sourceFields, int numSinkParts) {
        super(sourceFields, numSinkParts);
    }

    @SuppressWarnings ({"unchecked"})
    @Override
    public void sourceConfInit (FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        super.sourceConfInit(flowProcess, tap, conf);
        conf.setInputFormat(DeprecatedLzoTextInputFormat.class);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf ) {
        if (tap.getFullIdentifier(conf).endsWith(".lzo"))
        {
            throw new IllegalStateException("cannot write lzo files: "
                    + FileOutputFormat.getOutputPath(conf));
        }

        if (getSinkCompression() == Compress.DISABLE)
        {
            conf.setBoolean("mapred.output.compress", false);
        }
        else if (getSinkCompression() == Compress.ENABLE)
        {
            conf.setBoolean("mapred.output.compress", true);
            // be explicit about which compression class to use
            FileOutputFormat.setCompressOutput(conf, true);
            FileOutputFormat.setOutputCompressorClass(conf, LzopCodec.class);
        }

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setOutputFormat(TextOutputFormat.class);
    }

}