package NewsTop10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NewsMapper extends Mapper<LongWritable,Text,Text, NewsBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String[] spited = line.split("\t");
        String date = spited[0];
        String name = spited[2].replace("[","").replace("]","");
//        System.out.println(date2);
//        System.out.println(name);
//        System.out.println(new NewsBean(date,1L).toString());
        context.write(new Text(name),new NewsBean(date,1L));
    }
}
