package bigdataman.bellezza.babynames;

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import org.bson.Document;

public class InputOutputOperation
{
	private static BufferedWriter outFile = null;
	private static BufferedWriter logFile = null;

	private static Block<Document> printBlock = new Block<Document>() {
		public void apply(Document document) {
			System.out.println(document.toJson());
			try {
				InputOutputOperation.outFile.newLine();
				InputOutputOperation.outFile.write(document.toJson());
//				System.out.println(document.toJson());
				InputOutputOperation.outFile.flush();
			} catch (IOException e) { System.out.println("problema in output");
			}
		}
	};

	public static void outputAggregate(MongoCollection<Document> outputCollection, String name) {
		try {
			outFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/" + name + ".json", true), "UTF-8"));
		} catch (IOException e) {
			System.out.println("problema in output");
		}
		outputCollection.find().forEach(printBlock);
	}

	public static void helpCommand()
	{
		BufferedReader inFile = null;
		try
		{
			inFile = new BufferedReader(new InputStreamReader(new FileInputStream("input/help.txt")));
			String s; 
			while ((s = inFile.readLine()) != null) { 
				System.out.println(s);
			}
		}
		catch (IOException localIOException) {}
	}


	public static void updatelog(String operation, String[] param)
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		try
		{
			File log = new File("output/log"+Main.getDBName()+".txt");
			boolean exists = true;
			if (!log.exists())
				exists = false;
			logFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/log.txt", true), "UTF-8"));
			if (!exists)
				logFile.write(dateFormat.format(date) + "  \n*** LOG FILE --- DOCUMENT DATABASE  "+Main.getDBName().toUpperCase()+"***\n" + dateFormat.format(date) + "  *** Hadoop  -- VERSIONE 2.7.4\n" + dateFormat.format(date) + "  *** MongoDB -- VERSIONE 3.4.10\n\r\n\r");
			logFile.newLine();
			logFile.write(dateFormat.format(date) + " operazione: " + operation + " -- parametri: " + Arrays.toString(param));	
			logFile.flush();
		} catch (IOException e) {
			System.out.println("problema con file di log");
		}
	}


	public static void getLog()
	{
		BufferedReader inFile = null;
		try
		{
			inFile = new BufferedReader(new InputStreamReader(new FileInputStream("output/log.txt")));
			String s; 
			while ((s = inFile.readLine()) != null) { 
			System.out.println(s);
			}
		} catch (IOException e) { System.out.println("IMPOSSIBILE RECUPERARE IL FILE DI LOG");
		}
	}
}