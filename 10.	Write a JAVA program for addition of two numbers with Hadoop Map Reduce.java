import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;



public class Add {

  public static class M extends Mapper<LongWritable,Text,Text,IntWritable>{

    public void map(LongWritable k, Text v, Context c)throws java.io.IOException,InterruptedException{

      String[] a=v.toString().split(" ");

      c.write(new Text("Sum"), new IntWritable(Integer.parseInt(a[0]) + Integer.parseInt(a[1])));

    }

  }

  public static class R extends Reducer<Text,IntWritable,Text,IntWritable>{

    public void reduce(Text k, Iterable<IntWritable> v, Context c)throws java.io.IOException,InterruptedException{

      for(IntWritable i:v) c.write(k,i);

    }

  }

}

