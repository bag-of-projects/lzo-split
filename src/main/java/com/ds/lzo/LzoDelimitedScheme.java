package com.ds.lzo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextOutputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.hadoop.compression.lzo.LzopCodec;

/**
 * This adds LZO capabilities to the TextDelimited scheme
 */
@SuppressWarnings ({"UnusedDeclaration", "deprecation"})
public class LzoDelimitedScheme extends TextDelimited {
    private static final long serialVersionUID = 1L;
    private boolean hackSkipHeader = false;

    public LzoDelimitedScheme (Fields fields, String delimiter) {
        super(fields, delimiter);
    }

    public LzoDelimitedScheme (Fields fields, boolean skipHeader, String delimiter) {
        super(fields, skipHeader, delimiter);
        hackSkipHeader = skipHeader;
    }

    public LzoDelimitedScheme (Fields fields, String delimiter, Class[] types) {
        super(fields, delimiter, types);
    }

    public LzoDelimitedScheme (Fields fields, boolean skipHeader, String delimiter, Class[] types) {
        super(fields, skipHeader, delimiter, types);
        hackSkipHeader = skipHeader;
    }

    public LzoDelimitedScheme (Fields fields, String delimiter, String quote, Class[] types) {
        super(fields, delimiter, quote, types);
    }

    public LzoDelimitedScheme (Fields fields, boolean skipHeader, String delimiter, String quote, Class[] types) {
        super(fields, skipHeader, delimiter, quote, types);
        hackSkipHeader = skipHeader;
    }

    public LzoDelimitedScheme (Fields fields, String delimiter, String quote, Class[] types, boolean safe) {
        super(fields, delimiter, quote, types, safe);
    }

    public LzoDelimitedScheme (Fields fields, boolean skipHeader, String delimiter, String quote, Class[] types,
                               boolean safe) {
        super(fields, skipHeader, delimiter, quote, types, safe);
        hackSkipHeader = skipHeader;
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, String delimiter) {
        super(fields, sinkCompression, delimiter);
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter) {
        super(fields, sinkCompression, skipHeader, delimiter);
        hackSkipHeader = skipHeader;
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, String delimiter, Class[] types) {
        super(fields, sinkCompression, delimiter, types);
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter,
                               Class[] types) {
        super(fields, sinkCompression, skipHeader, delimiter, types);
        hackSkipHeader = skipHeader;
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, String delimiter, Class[] types, boolean safe) {
        super(fields, sinkCompression, delimiter, types, safe);
    }

    public LzoDelimitedScheme (Fields fields, String delimiter, String quote) {
        super(fields, delimiter, quote);
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, String delimiter, String quote) {
        super(fields, sinkCompression, delimiter, quote);
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, String delimiter, String quote, Class[] types) {
        super(fields, sinkCompression, delimiter, quote, types);
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, String delimiter, String quote, Class[] types,
                               boolean safe) {
        super(fields, sinkCompression, delimiter, quote, types, safe);
    }

    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter,
                               boolean strict, String quote, Class[] types, boolean safe) {
        super(fields, sinkCompression, skipHeader, writeHeader, delimiter, strict, quote, types, safe);
        hackSkipHeader = skipHeader;
    }
    public LzoDelimitedScheme (Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, String quote,Class[] types, boolean safe) {
        super(fields, sinkCompression, hasHeader, delimiter, quote, types, safe);
    }


    @SuppressWarnings ({"unchecked"})
    @Override
    public void sourceConfInit (FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        super.sourceConfInit(flowProcess, tap, conf);
        conf.setInputFormat(DeprecatedLzoTextInputFormatForCombined.class);
        conf.set("cascading.hadoop.hfs.combine.files","true");
    }


    @Override
    public void sinkConfInit (FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap,JobConf conf) {

        if (tap.getFullIdentifier(conf).endsWith(".lzo"))
        {
            throw new IllegalStateException("cannot write lzo files: "
                    + FileOutputFormat.getOutputPath(conf));
        }


        conf.setBoolean("mapred.output.compress", true);
        // be explicit about which compression class to use
        FileOutputFormat.setCompressOutput(conf, true);
        FileOutputFormat.setOutputCompressorClass(conf, LzopCodec.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setOutputFormat(TextOutputFormat.class);
    }

    @SuppressWarnings ("unchecked")
    @Override
    public Fields retrieveSourceFields (FlowProcess<JobConf> flowProcess, Tap tap) {

        if( !hackSkipHeader || !getSourceFields().isUnknown() ) {
            return getSourceFields();
        }

        // no need to open them all
        if( tap instanceof CompositeTap) {
            tap = (Tap) ( (CompositeTap) tap ).getChildTaps().next();
        }

        // should revert to file:// (Lfs) if tap is Lfs
        tap = new Hfs( new LzoTextLine( new Fields( "line" ) ), tap.getFullIdentifier( flowProcess.getConfigCopy() ) );

        Fields foundFields = delimitedParser.parseFirstLine(flowProcess, tap);
        setSourceFields( foundFields );

        return getSourceFields();
    }
}
