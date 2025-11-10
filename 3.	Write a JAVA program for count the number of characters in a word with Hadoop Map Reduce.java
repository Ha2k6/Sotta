Import org.apache.hadoop.io.*;import org.apache.hadoop.mapreduce.*;import org.apache.hadoop.conf.*;import org.apache.hadoop.fs.*;import org.apache.hadoop.mapreduce.lib.input.*;import org.apache.hadoop.mapreduce.lib.output.*;import java.io.*;

Public class CC{

 Public static class M extends Mapper<LongWritable,Text,Text,IntWritable>{

  Public void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{

   String w=v.toString();c.write(new Text(w),new IntWritable(w.length()));

  }

 }

 Public static class R extends Reducer<Text,IntWritable,Text,IntWritable>{

  Public void reduce(Text k,Iterable<IntWritable> v,Context c)throws IOException,InterruptedException{

   c.write(k,v.iterator().next());

  }

 }

 Public static void main(String[] a)throws Exception{

  Job j=Job.getInstance(new Configuration(),”cc”);

  j.setJarByClass(CC.class);

  j.setMapperClass(M.class);j.setReducerClass(R.class);

  j.setOutputKeyClass(Text.class);j.setOutputValueClass(IntWritable.class);

  FileInputFormat.addInputPath(j,new Path(a[0]));

  FileOutputFormat.setOutputPath(j,new Path(a[1]));

  j.waitForCompletion(true);

 }

}

