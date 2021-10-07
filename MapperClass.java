package bigdataman.bellezza.babynames;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class MapperClass 	extends Mapper<Object, BSONObject, Text, IntWritable>
{
	private IntWritable count = new IntWritable(1);


	public void map(Object key, BSONObject val, Mapper<Object, BSONObject, Text, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
//		System.out.println("METODO MAP_________________________________");
		Configuration conf = context.getConfiguration();
		String param = conf.get("param");
		String[] arg = param.split(",");
		String[] keyVal= new String[1];

		boolean check = true;
		System.out.println("__colonna di count = "+arg[arg.length-1]);
		if(arg[arg.length-1].split("=").length>1) {
			if(arg[arg.length-1].split("=")[0].equals("count_option")) {
				this.count = new IntWritable(Integer.parseInt((String)val.get(arg[arg.length-1].split("=")[1])));
				System.out.println("COUNT: "+count);
			}else {
				System.out.println("ERROR: camp name doesn't exist");
			}
			arg = (String[])ArrayUtils.subarray(arg, 0, arg.length-1);
		}

		if(arg.length>1) {
			keyVal = new String[arg.length-1];
			for(int i = 0; i< keyVal.length;i++) {
				keyVal[i] = (String)val.get(arg[i].split("=")[0]);
//				System.out.println("key val : "+keyVal[i]);
//				System.out.println(" =? arg: "+arg[i].split("=")[1]);
			}
			for (int m = 0; m<keyVal.length;m++) {
				if(keyVal[m]==null || !keyVal[m].equals(arg[m].split("=")[1])){
					check=false;
//					System.out.println("key val : "+keyVal[m]+" =? arg: "+arg[m].split("=")[1]);
				}
			}
		}
		String newKey = "";
		newKey = (String)val.get(arg[arg.length-1]);
//		System.out.println("key aggr : "+newKey);


		//		if (arg[2].split(":").length == 2) {
		//			BasicDBObject newObj = (BasicDBObject)val.get(arg[2].split(":")[0]);
		//			newKey = newObj.getString(arg[2].split(":")[1]);
		//		} else {
		//			newKey = (String)val.get(arg[2]);
		//		}
		//
		//		if (arg[0].split(":").length == 2) {
		//			BasicDBObject obj = (BasicDBObject)val.get(arg[0].split(":")[0]);
		//			keyVal = obj.getString(arg[0].split(":")[1]);
		//		} else {
		//			keyVal = (String)val.get(arg[0]);
		//		}


		if (check) {
			context.write(new Text(newKey), this.count);
//			System.out.println("condiczione check: "+keyVal[0]);
		}
	}
}
