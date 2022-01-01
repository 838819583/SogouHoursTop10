package NewsTop10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NewsPartitioner extends Partitioner<Text,NewsBean> {
    int partitionNum;


    @Override
    public int getPartition(Text text,NewsBean newsBean, int i) {
        String date = newsBean.getDate();
        System.out.println(date);
        partitionNum = Integer.parseInt(date.split(":")[0]);
        return partitionNum;
    }
}
