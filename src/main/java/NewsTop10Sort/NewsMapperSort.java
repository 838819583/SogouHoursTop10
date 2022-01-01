package NewsTop10Sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NewsMapperSort extends Mapper<LongWritable,Text, LongWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"UTF-8");
        String[] spited = line.split("\t");
        String name = spited[0];
        String date = spited[1];
        Long count = Long.parseLong(spited[2]);
//        System.out.println(new NewsBean(name,count));
//        System.out.println(count);
        context.write(new LongWritable(count),new Text(date+"\t"+name));

    }
}
