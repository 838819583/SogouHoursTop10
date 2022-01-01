package NewsTop10Sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NewsComparator extends WritableComparator {
    public  NewsComparator() {
        super(LongWritable.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b);
    }
}
