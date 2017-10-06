// Two phase matrix multiplication in Hadoop MapReduce
// Template file for homework #1 - INF 553 - Spring 2017
// - Wensheng Wu

import java.io.IOException;

// add your import statement here if needed
// you can only import packages from java.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.List;
import java.util.ArrayList;


public class TwoPhase {

    // mapper for processing entries of matrix A
    public static class PhaseOneMapperA 
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    String [] in = value.toString().split(",");
		String k = in[1];
		String v = "A,"+ in[0]+","+in[2];
		outKey = new Text(k);
		outVal = new Text(v);
		context.write(outKey, outVal);
	}

    }

    // mapper for processing entries of matrix B
    public static class PhaseOneMapperB
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    String [] in = value.toString().split(",");
		String k = in[0];
		String v = "B,"+ in[1]+","+in[2];
		outKey = new Text(k);
		outVal = new Text(v);
		context.write(outKey, outVal);

	}
    }

    public static class PhaseOneReducer
	extends Reducer<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outVal = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
	    
	    List<Integer> listA1 = new ArrayList<Integer>();
		List<Integer> listB1 = new ArrayList<Integer>();
		List<Integer> listA2 = new ArrayList<Integer>();
		List<Integer> listB2 = new ArrayList<Integer>();
		for(Text s: values)
		{
			
			String st = s.toString();
			String[] s1 = st.split(",");
			if(s1[0].equals("A"))	
			{
				listA1.add(Integer.parseInt(s1[2]));
				listA2.add(Integer.parseInt(s1[1]));
				
			}
			else
			{			
				listB1.add(Integer.parseInt(s1[2]));
				listB2.add(Integer.parseInt(s1[1]));
				
			}
			
				
		}
		
		String k;
		int val;
		for(int i=0; i<listA1.size(); i++)
		{
			for(int j=0; j<listB1.size();j++)
			{
				k = listA2.get(i)+","+listB2.get(j);
				val = listA1.get(i)*listB1.get(j);
				
				
				outKey = new Text(k);
				outVal = new Text(Integer.toString(val));
				context.write(outKey, outVal);
			}
		}

	}

    }

    public static class PhaseTwoMapper 
	extends Mapper<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(Text key, Text value, Context context)
	    throws IOException, InterruptedException {

	    context.write(key, value);

	}
    }

    public static class PhaseTwoReducer 
	extends Reducer<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	 
	    // fill in your code
		int sum1=0;
		for(Text s: values)
		{
			int n = Integer.parseInt(s.toString());
			sum1 = sum1+n;
		}
		
		
		outKey =key;
		outVal = new Text(Integer.toString(sum1));
		
		context.write(outKey, outVal);

	}
    }


public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job jobOne = Job.getInstance(conf, "phase one");

	jobOne.setJarByClass(TwoPhase.class);

	jobOne.setOutputKeyClass(Text.class);
	jobOne.setOutputValueClass(Text.class);

	jobOne.setReducerClass(PhaseOneReducer.class);

	MultipleInputs.addInputPath(jobOne,
				    new Path(args[0]),
				    TextInputFormat.class,
				    PhaseOneMapperA.class);

	MultipleInputs.addInputPath(jobOne,
				    new Path(args[1]),
				    TextInputFormat.class,
				    PhaseOneMapperB.class);

	Path tempDir = new Path("temp");

	FileOutputFormat.setOutputPath(jobOne, tempDir);
	jobOne.waitForCompletion(true);


	// job two
	Job jobTwo = Job.getInstance(conf, "phase two");
	

	jobTwo.setJarByClass(TwoPhase.class);

	jobTwo.setOutputKeyClass(Text.class);
	jobTwo.setOutputValueClass(Text.class);

	jobTwo.setMapperClass(PhaseTwoMapper.class);
	jobTwo.setReducerClass(PhaseTwoReducer.class);

	jobTwo.setInputFormatClass(KeyValueTextInputFormat.class);

	FileInputFormat.setInputPaths(jobTwo, tempDir);
	FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));
	
	jobTwo.waitForCompletion(true);
	
	FileSystem.get(conf).delete(tempDir, true);
	
    }
}