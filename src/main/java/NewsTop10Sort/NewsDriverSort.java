package NewsTop10Sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NewsDriverSort {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(NewsDriverSort.class);

        //输入数据
        FileInputFormat.setInputPaths(job, args[0]);

        //指定mapper类
        job.setMapperClass(NewsMapperSort.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //指定Partitioner类，指定Reducer任务个数
        job.setPartitionerClass(NewsPartitioner.class);
        job.setNumReduceTasks(24);

        //指定自定义Comparator类
        job.setSortComparatorClass(NewsComparator.class);

        //指定reducer类
        job.setReducerClass(NewsReducerSort.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //输出数据
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //true表示将运行进度等信息及时输出给用户
        job.waitForCompletion(true);
    }
}
