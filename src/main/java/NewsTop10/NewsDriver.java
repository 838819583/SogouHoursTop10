package NewsTop10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NewsDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(NewsDriver.class);

        //输入数据
        FileInputFormat.setInputPaths(job, args[0]);

        //指定mapper类
        job.setMapperClass(NewsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NewsBean.class);

        //指定Partitioner类，指定Reducer任务个数
        job.setPartitionerClass(NewsPartitioner.class);
        job.setNumReduceTasks(24);

        //指定reducer类
        job.setReducerClass(NewsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NewsBean.class);

        //输出数据
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //true表示将运行进度等信息及时输出给用户
        job.waitForCompletion(true);
    }
}
