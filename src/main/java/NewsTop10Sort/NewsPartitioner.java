package NewsTop10Sort;

import NewsTop10.NewsBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NewsPartitioner extends Partitioner<LongWritable, Text> {
    int partitionNum;


    @Override
    public int getPartition(LongWritable longWritable, Text text, int i) {
        String date = text.toString().split("\t")[0];
        partitionNum=Integer.parseInt(date.split(":")[0]);
        return partitionNum;

    }
}
