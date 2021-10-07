package bigdataman.bellezza.babynames;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.bson.Document;

public class Main {
	private static String dbname;

	private static MongoClient client;
	private static MongoDatabase database;
	private static MongoCollection<Document> collection;



	public static void main(String[] args)
	{
//		System.out.println("args: "+args[1]);
		if (args.length != 2) {
			if(dbname==null) {
				throw new IllegalArgumentException("Inserisci nome database");
			}else{
				throw new IllegalArgumentException("Inserisci un'operazione");
			}
		}


		Main.setDBName(args[1]);
		String[] stri = new String[1];
		stri[0] = "aggregate";

		initSystem();
		System.out.println("init system, dbname: " + dbname);
		while(true) {
			BufferedReader in = null;
			try {
				in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
			} catch (IOException localIOException) {}
			String str = null;
			try
			{
				str = in.readLine(); } catch (IOException localIOException1) {}
			String[] tmp = str.split(" ");



			if (tmp[0].equals("aggregate")) {
				System.out.println("   ---------------------  \n\r   AGGREGAZIONE MapReduce\n\r   ---------------------\n\rInserisci: <aggregation_term>\n\roppure: <key_condition> <value_condition> <aggregation_term>\n\r");


				String s = "";
				String collectionName = "";
				if (tmp.length == 2) {
					s = tmp[1];
					collectionName = s;
				} else {
					if(tmp[tmp.length-1].split("=").length>1) {
						s =tmp[1];
						for(int l = 2; l<tmp.length;l++) {
							s += ","+tmp[l];
						}
						collectionName = tmp[tmp.length-2]+"_"+tmp[tmp.length-1];
						if(tmp.length>3) {
							collectionName += "_with";
						}
						for(int k=1;k<tmp.length-2;k++) {
							collectionName += "_" + tmp[k];
						}
					}else {
						s =tmp[1];
						for(int l = 2; l<tmp.length;l++) {
							s += ","+tmp[l];
						}
						collectionName = tmp[tmp.length-1]+"_with";
						for(int k=1;k<tmp.length-1;k++) {
							collectionName += "_" + tmp[k];
						}
					}
				}
				
				
				System.out.println("  -- Operazione in corso ...");
//				System.out.println("  stringhe  "+s+" "+collectionName);

				try {
					ToolRunner.run(new MapReduceTool(s, collectionName), tmp);
				}
				catch (Exception localException) {
					System.out.println("ERRORE MAPREDUCE RUN");
				}
				MongoCollection<Document> outputCollection = database.getCollection("aggregation_" + collectionName);
				InputOutputOperation.outputAggregate(outputCollection, "aggregation_" + collectionName);

				InputOutputOperation.updatelog("AGGREGATE", (String[])ArrayUtils.subarray(tmp, 1, tmp.length));


			}
			else if (tmp[0].equals("find")) {
				System.out.println("   ---------------------  \n\r   RICERCA Elemento\n\r   ---------------------  \n\rInserisci: <key> <value>\n\r");

				try
				{
					String value = tmp[3];
					for (int i = 4; i < tmp.length; i++)
						value = value + " " + tmp[i];
					Finder.findElement(collection, tmp[1], value);
				} catch (ArrayIndexOutOfBoundsException E) { System.out.println("Formato input non valido");
				}
				InputOutputOperation.updatelog("FIND", (String[])ArrayUtils.subarray(tmp, 1, tmp.length));


			}
			else if (tmp[0].equals("help")) {
				InputOutputOperation.helpCommand();


			}
			else if (tmp[0].equals("log")) {
				InputOutputOperation.getLog();

			}
			else if (tmp[0].equals("exit")) {
				break;

			}
			else
			{
				System.out.println("Comando errato, digita \n\r\n\r    sh run.sh help \n\r\n\rper avere informazioni");
			}
			System.out.println();
			System.out.println("   ---------------------  \n\r   COMPLETATO\n\r   ---------------------  ");

		}
	}

	public static String getDBName() {
		return dbname;
	}
	
	public static void setDBName(String s) {
		dbname=s;
	}

	private static void initSystem()
	{
		LogManager.getLogger("org.mongodb.driver.connection").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.management").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.cluster").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.protocol.insert").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.protocol.query").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.protocol.update").setLevel(Level.ERROR);
		try {
			client = new MongoClient("localhost", 27017);
			database = client.getDatabase(Main.getDBName());
			collection = database.getCollection(Main.getDBName());
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}


}
