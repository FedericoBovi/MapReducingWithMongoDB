package bigdataman.bellezza.babynames;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;


public class Finder {

	private static Block<Document> blockDoc =new Block<Document>() {

		public void apply(Document document) {		
				System.out.println(document.toJson());		
		}

	};

	public static void findElement(MongoCollection<Document> collection, 
			String key, String value) {
		collection.find(Filters.eq(key, value)).forEach(blockDoc);
	}
}
