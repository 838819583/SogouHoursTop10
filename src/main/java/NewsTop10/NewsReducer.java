package NewsTop10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NewsReducer extends Reducer<Text, NewsBean,Text, NewsBean> {
    Long count;
    String date;
    @Override
    protected void reduce(Text key, Iterable<NewsBean> values, Context context) throws IOException, InterruptedException {
        count=0L;
        for (NewsBean value : values) {
            count = count+value.getCount();
            date=value.getDate();
            System.out.println(count);
            System.out.println(date);
        }
        context.write(key,new NewsBean(date,count));
    }
}
