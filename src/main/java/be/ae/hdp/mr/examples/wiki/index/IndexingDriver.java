package be.ae.hdp.mr.examples.wiki.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import be.ae.hdp.mr.examples.common.Utils;

public class IndexingDriver extends Configured implements Tool {
	private static final String appName = "WikiIndexing";
	private static Logger logger = Logger.getLogger(IndexingDriver.class);

	public static void main(String[] args) throws Exception {
		// This is the root logger provided by log4j
		logger = Logger.getRootLogger();
		logger.setLevel(Level.INFO);

		// Define log pattern layout
		PatternLayout layout = new PatternLayout(
				"%d{ISO8601} [%t] %-5p %c %x - %m%n");

		// Add console appender to root logger
		logger.addAppender(new ConsoleAppender(layout));

		try {
			logger.addAppender(new FileAppender(layout, Utils
					.getUniqueLogFolder(appName, "output") + "/" + "log.txt"));
		} catch (IOException e) {
			logger.error("Could not find logging location");
			e.printStackTrace();
		}

		ToolRunner.run(new IndexingDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}
}
