package bigdataman.bellezza.babynames;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class MapReduceTool
extends MongoTool
{
	public MapReduceTool(String s, String collectionName)
	{
		JobConf conf = new JobConf(new Configuration());
		conf.set("param", s);

		MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);

		MongoConfig config = new MongoConfig(conf);


		config.setMapper(MapperClass.class);
		
		config.setReducer(ReducerClass.class);
		
		config.setMapperOutputKey(Text.class);
		config.setMapperOutputValue(IntWritable.class);
		config.setOutputKey(BSONWritable.class);
		config.setOutputValue(IntWritable.class);
		config.setInputURI("mongodb://localhost:27017/"+Main.getDBName()+"."+Main.getDBName());
		config.setOutputURI("mongodb://localhost:27017/"+Main.getDBName()+".aggregation_" + collectionName);
		setConf(conf);
		
		System.out.println("mapredtool DONE");
	}
}