package com.javacodegeeks.examples.hadoopMapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a single line at a time, it splits the line based on space
 * and generated the token which are output by map with value as one to be consumed
 * by reduce class
 * 
 * @author Raman
 */
public class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
	 
	private final static IntWritable one = new IntWritable(1);
    private Text selectedLine = new Text();
    private IntWritable noOfPageVisited = new IntWritable();
    
    /**
     * map function of Mapper parent class takes a line of text at a time
     * splits to tokens and passes to the context as word along with value as one
     */
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] pagesVisited = line.split(" ");
		
		if(pagesVisited.length > 500) {
			selectedLine.set(line);
			noOfPageVisited.set(pagesVisited.length);
			context.write(selectedLine, noOfPageVisited);
		}
	}
}
