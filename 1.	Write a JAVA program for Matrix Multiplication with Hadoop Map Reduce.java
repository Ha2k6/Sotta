import org.apache.hadoop.io.*;import org.apache.hadoop.mapreduce.*;import org.apache.hadoop.conf.*;import org.apache.hadoop.fs.*;import org.apache.hadoop.mapreduce.lib.input.*;import org.apache.hadoop.mapreduce.lib.output.*;import java.io.*;

public class MM{

 public static class M extends Mapper<LongWritable,Text,Text,Text>{

  public void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{

   String[] a=v.toString().split(",");

   c.write(new Text(a[1]+","+a[2]),new Text(a[0]+","+a[3]));

  }

 }

 public static class R extends Reducer<Text,Text,Text,Text>{

  public void reduce(Text k,Iterable<Text> v,Context c)throws IOException,InterruptedException{

   int A=0,B=0;

   for(Text t:v){

    String[] x=t.toString().split(",");

    if(x[0].equals("A"))A=Integer.parseInt(x[1]);

    else B=Integer.parseInt(x[1]);

   }

   c.write(k,new Text((A*B)+""));

  }

 }

 public static void main(String[] a)throws Exception{

  Job j=Job.getInstance(new Configuration(),"x");

  j.setJarByClass(MM.class);

  j.setMapperClass(M.class);j.setReducerClass(R.class);

  j.setOutputKeyClass(Text.class);j.setOutputValueClass(Text.class);

  FileInputFormat.addInputPath(j,new Path(a[0]));

  FileOutputFormat.setOutputPath(j,new Path(a[1]));

  j.waitForCompletion(true);

 }

}

