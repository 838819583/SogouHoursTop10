package NewsTop10Sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NewsReducerSort extends Reducer<LongWritable,Text,Text,LongWritable> {
    int i =1;
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (i<10){
            for (Text value : values) {
                context.write(value,key);
                i+=1;
            }

    }}

}
