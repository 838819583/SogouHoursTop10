package NewsTop10;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NewsBean implements Writable, WritableComparable<NewsBean> {
    private String date;
    private Long count;

    public NewsBean(){super();}

    public NewsBean(String date, Long count){
        super();
        this.date = date;
        this.count = count;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getDate() {
        return date;
    }

    public Long getCount() {
        return count;
    }
//序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(date);
        dataOutput.writeLong(count);
    }
//反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.date= dataInput.readUTF();
        this.count= dataInput.readLong();

    }

    @Override
    public String toString() {
        return date + '\t' + count;
    }

    @Override
    public int compareTo(NewsBean o) {
        if (this.count>o.count){
            return -1;
        }else{
            return 1;
        }
    }
}
