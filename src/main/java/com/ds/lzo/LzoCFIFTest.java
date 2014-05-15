package com.ds.lzo;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProps;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.test.HadoopPlatform;
import cascading.test.TestPlatform;
import cascading.tuple.Fields;

public class LzoCFIFTest {
    private static final int MAX_CONCUR_STEPS = 6;

    /**
     * Parse the args from the command line, and store the results in _commandLine
     */
    protected static CommandLine parseCommandLine( String[] args, Options options )
    {
        CommandLineParser parser = new PosixParser();

        try
        {
            return( parser.parse( options, args ) );
        }
        catch (org.apache.commons.cli.ParseException e)
        {
            HelpFormatter help = new HelpFormatter();
            help.printHelp(" ", "", getOptions(), "\n" + e.getMessage());
            return null;
        }
    }

    /**
     * Get the command line arguments.
     *
     */
    @SuppressWarnings("static-access")
    protected static Options getOptions ()
    {
        Options options = new Options();

        // Required options
        options.addOption(OptionBuilder.withArgName("in").isRequired(true).hasArg(true).create("in"));
        options.addOption(OptionBuilder.withArgName("out").isRequired(true).hasArg(true).create("out"));

        return options;
    }

    /**
     * Main.
     * @throws IOException
     */
    public static void main (String[] args) throws IOException
    {
        LzoCFIFTest driver = new LzoCFIFTest();
        driver.go(args);
    }

    public void go(String[] args) throws IOException
    {
//        String inPath = args[ 1 ];
//        String outPath = args[ 2 ];

        // Parse options from command line.
        CommandLine commandLine = parseCommandLine(args, getOptions());
        if (null == commandLine) { return; }

        String inPath = commandLine.getOptionValue("in");
        String outPath = commandLine.getOptionValue("out");

        Properties properties = new Properties();
        initConfig(properties);
        AppProps.setApplicationJarClass(properties, LzoCFIFTest.class);
        FlowConnector flowConnector = new HadoopFlowConnector(properties);

        // create source and sink taps
        Tap inTap = getLzoTextFileTap(inPath, new Fields("text"), "\t", null);
        Tap outTap = new Hfs( new TextDelimited( true, "\t" ), outPath, SinkMode.REPLACE);

        // specify a regex operation to split the "document" text lines into a token stream
        Fields token = new Fields( "token" );
        Fields text = new Fields( "text" );
        RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
        // only returns "token"
        Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );

        // determine the word counts
        Pipe wcPipe = new Pipe( "wc", docPipe );
        wcPipe = new GroupBy( wcPipe, token );
        wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

        Flow flow = flowConnector.connect("wordCount", inTap, outTap, wcPipe);
        flow.complete();

        System.out.println("all finished!");
    }

    public void initConfig(Properties props) {
      //enable lzo
        props.setProperty("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec");
        props.setProperty("io.compression.codec.lzo.class","com.hadoop.compression.lzo.LzoCodec");


        //props.setProperty("io.seqfile.compress.blocksize","1000000");

        //Low priority
        props.setProperty("mapred.job.priority","LOW");

        enableCompression( props );

        setMapCodec(props,"com.hadoop.compression.lzo.LzoCodec" );
        setReduceCodec(props,"com.hadoop.compression.lzo.LzoCodec" );

        //Turn this on when we get combined LZO files to work.. and things will go WAY faster.
        //props.setProperty("cascading.hadoop.hfs.combine.files","true");
        props.setProperty("mapred.min.split.size","134217728"); // 128 MB
//        props.setProperty("mapred.max.split.size","268435456"); // 256 MB
//        props.setProperty("mapred.max.split.size","536870912"); // 512 MB

        props.setProperty("cascading.cogroup.spill.threshold","1000000");
        props.setProperty("cascading.spill.threshold","1000000");
        props.setProperty("cascading.spill.list.threshold","10000000");
        props.setProperty("cascading.spill.map.threshold","10000000");

        props.setProperty("cascading.cogroup.spill.compress","true");
        props.setProperty("cascading.spill.compress","true");

        props.setProperty("cascading.cogroup.spill.codecs","com.hadoop.compression.lzo.LzoCodec,org.apache.hadoop.io.compress.GzipCodec");
        props.setProperty("cascading.spill.codecs","com.hadoop.compression.lzo.LzoCodec,org.apache.hadoop.io.compress.GzipCodec");

        //props.setProperty("dfs.permissions","true");
        props.setProperty("dfs.umask","002");
        props.setProperty("dfs.umaskmode","002");
        props.setProperty("fs.permissions.umask-mode","002");
        props.setProperty("mapreduce.job.userhistorylocation","none");
        props.setProperty("mapred.job.userhistorylocation","none");
        props.setProperty("hadoop.job.history.user.location","none");
        FlowProps.setMaxConcurrentSteps(props, MAX_CONCUR_STEPS);
        initLog4j(props);
    }

    public void enableCompression(Properties props ) {
        props.setProperty("mapred.output.compression.type","BLOCK");
        props.setProperty("maprededuce.output.compression.type","BLOCK");

        props.setProperty("maprededuce.map.output.compression.type","BLOCK");
        props.setProperty("mapred.map.output.compression.type","BLOCK");


        props.setProperty("mapred.compress.map.output","true");
        props.setProperty("mapreduce.compress.map.output","true");
        props.setProperty("mapreduce.map.output.compress","true");
        props.setProperty("mapred.map.output.compress","true");

        props.setProperty("mapred.compress.output","true");
        props.setProperty("mapreduce.compress.output","true");
        props.setProperty("mapreduce.output.compress","true");
        props.setProperty("mapred.output.compress","true");
    }

    public void setMapCodec(Properties props, String codec) {
        props.setProperty("mapred.map.output.compression.codec",codec);
        props.setProperty("maprededuce.map.output.compress.codec",codec);
    }

    public static void setReduceCodec(Properties props, String codec) {
        props.setProperty("mapred.output.compression.codec",codec);
        props.setProperty("maprededuce.output.compress.codec",codec);
    }

    public void initLog4j(Properties props ) {
        props.setProperty("log4j.logger","cascading.tuple.hadoop.TupleSerialization=WARN");
    }

    public static Tap getLzoTextFileTap (String fullFilename, Fields fields, String delimiter,String quote)
    {
        LzoDelimitedScheme scheme = new LzoDelimitedScheme(fields,
                                                           TextLine.Compress.ENABLE,
                                                           false,
                                                           false,
                                                           delimiter,
                                                           true,
                                                           quote,
                                                           null,
                                                           false);
        TestPlatform platform = new HadoopPlatform();
        return( platform.getTap( scheme, fullFilename, SinkMode.REPLACE ) );
    }

}
