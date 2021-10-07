package bigdataman.bellezza.babynames;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

public class ReducerClass extends Reducer<Text, IntWritable, BSONWritable, IntWritable>
{
	private BSONWritable reduceResult = new BSONWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, BSONWritable, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		String param = conf.get("param");
		String[] args = param.split(",");
		String s;
		
		s = args[args.length-1];
		
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		BSONObject outDoc = BasicDBObjectBuilder.start().add(s, key.toString()).get();
		this.reduceResult.setDoc(outDoc);
		context.write(this.reduceResult, new IntWritable(sum));
	}
}