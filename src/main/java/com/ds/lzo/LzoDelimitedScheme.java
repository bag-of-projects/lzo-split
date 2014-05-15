package com.ds.lzo;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * This adds LZO capabilities to the TextDelimited scheme
 */
@SuppressWarnings ({"UnusedDeclaration", "deprecation"})
public class LzoDelimitedScheme extends TextDelimited {
    private static final long serialVersionUID = 1L;
    private boolean hackSkipHeader = false;

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter,
                               boolean strict, String quote, Class[] types, boolean safe) {
        super(fields, sinkCompression, skipHeader, writeHeader, delimiter, strict, quote, types, safe);
        hackSkipHeader = skipHeader;
    }

    @SuppressWarnings ({"unchecked"})
    @Override
    public void sourceConfInit (FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        super.sourceConfInit(flowProcess, tap, conf);
        conf.setInputFormat(DeprecatedLzoTextInputFormatForCombined.class);
        conf.set("cascading.hadoop.hfs.combine.files","true");
    }

}
