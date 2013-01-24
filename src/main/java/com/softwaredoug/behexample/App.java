package com.softwaredoug.behexample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void writeBooks() throws Exception {
	   BehemothFile behFile = null;
        try {
        	behFile = new BehemothFile("/tmp/books.seq");
        
	        // Add multiple documents
	        HashMap<String, ArrayList<String>> fields = new HashMap<String, ArrayList<String>>();
	        
	        // An authors field
	        ArrayList<String> authors = new ArrayList<String>();
	        authors.add("Doug Turnbull");
	        authors.add("Nassim Talem");
	        fields.put("author", authors);
	        
	        // Publishers field
	        ArrayList<String> publisher = new ArrayList<String>();
	        publisher.add("Foo Publications");
	        fields.put("publisher", publisher);
	        
	        // The full text of the document, what will be mapped to "text"
	        String bookBody = "It was a dark and stormy night...";
	        
	        // Add to the document
	        behFile.addDoc(fields, bookBody, bookBody, /*id:*/"DarkAndStormy");
	        
	        // Another book...
	        authors.clear();
	        authors.add("Herman Melville");
	        fields.put("author", authors);
	        
	        publisher.clear();
	        publisher.add("Bar Enterprises");
	        fields.put("publisher", publisher);
	        
	        bookBody = "Call me Ahab...";
	        
	        behFile.addDoc(fields, bookBody, bookBody, "MobyDick");
	        
        }
        finally {
        	if (behFile != null) {
        		behFile.close();
        	}
        }
	}
	
	public static void readBackBehFile() throws IOException {
		// Readback using Hadoop IO classes + Behemoth lib
		Configuration conf = new Configuration();
		Path sourcePath = new Path("/tmp/books.seq");
		FileSystem fs =  sourcePath.getFileSystem(conf);
		SequenceFile.Reader seqFile = new SequenceFile.Reader(fs, sourcePath, conf);		

		Text key = new Text();
		BehemothDocument value = new BehemothDocument();
		while (seqFile.next(key, value))
		{
			System.out.println(key);
			List<Annotation> annots = value.getAnnotations();
			for (Annotation annot: annots) {
				Map<String, String> feature = annot.getFeatures();
				assert(feature.size() == 1);
				System.out.println("Annotation: " + annot.getType());
				for (Map.Entry<String, String> entry: feature.entrySet()) {
					System.out.println("Field: " + entry.getKey() + " Value: " + entry.getValue());
				}
			}
		}
		
		seqFile.close();
		
	}
	
    public static void main( String[] args ) throws Exception
    {
    	writeBooks();
    	readBackBehFile();
    }
}
