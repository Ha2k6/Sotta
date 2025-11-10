import org.apache.hadoop.io.*;import org.apache.hadoop.mapreduce.*;import org.apache.hadoop.conf.*;import org.apache.hadoop.fs.*;import org.apache.hadoop.mapreduce.lib.input.*;import org.apache.hadoop.mapreduce.lib.output.*;import java.io.*;

public class WC{

 public static class M extends Mapper<LongWritable,Text,Text,IntWritable>{

  public void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{

   for(String w:v.toString().split("\\s+"))c.write(new Text(w),new IntWritable(1));

  }

 }

 public static class R extends Reducer<Text,IntWritable,Text,IntWritable>{

  public void reduce(Text k,Iterable<IntWritable> v,Context c)throws IOException,InterruptedException{

   int s=0;for(IntWritable i:v)s+=i.get();c.write(k,new IntWritable(s));

  }

 }

 public static void main(String[] a)throws Exception{

  Job j=Job.getInstance(new Configuration(),"wc");

  j.setJarByClass(WC.class);

  j.setMapperClass(M.class);j.setReducerClass(R.class);

  j.setOutputKeyClass(Text.class);j.setOutputValueClass(IntWritable.class);

  FileInputFormat.addInputPath(j,new Path(a[0]));

  FileOutputFormat.setOutputPath(j,new Path(a[1]));

  j.waitForCompletion(true);

 }

}
