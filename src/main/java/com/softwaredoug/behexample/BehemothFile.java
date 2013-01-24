package com.softwaredoug.behexample;

import java.io.IOException;
import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * @author doug turnbull
 * 
 * Wraps a sequence file for behemoth
 *
 */
public class BehemothFile implements Closeable {

	private SequenceFile.Writer writer;
	private boolean isClosed;

	public BehemothFile(String filePath) throws IOException {
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fs = path.getFileSystem(conf);
		writer = SequenceFile.createWriter(fs, conf, path, Text.class,
				BehemothDocument.class);
		isClosed = false;	
	}
	
	/**
	 * 
	 * @param fields
	 *            multivalue map of fields to be ingested as
	 *            annotations_<Key>_value -> <value> where value is the key in
	 *            the map and value is one of the values in the corresponding
	 *            list
	 * @param content
	 *            raw content, typically this is the same as "text"
	 * @param text
	 *            full document text -- mapped to the "text" field in LWBD
	 * @param urlId
	 *            id for the doc, used as both the key and as the "URL" for the
	 *            behemoth doc
	 * @throws IOException
	 */
	public void addDoc(Map<String, ? extends Collection<String>> fields, String content,
			String text, String urlId) throws IOException {
		if (content == null || text == null || urlId == null) {
			throw new NullPointerException("content is null");
		}
		
		// Because the behemoth library does odd things and doesn't
		// seem to protect you from the file already being closed
		if (isClosed) {
			throw new IOException("Behemoth file closed..");
		}
		
		for (Map.Entry<String, ? extends Collection<String>> currField : fields.entrySet()) {
			if (currField.getValue() == null) {
				throw new NullPointerException("Field: " + currField.getKey() + " is null");
			}
		}
		
		// Create a single behemoth document, logically a "row" in the overall
		// Behemoth sequency file
		BehemothDocument doc = new BehemothDocument();
		
		// Global information for the behemoth document
		doc.setContent(content.getBytes("UTF-8"));
		doc.setText(text);
		doc.setUrl(urlId);
		Text key = new Text();
		key.set(urlId.getBytes("UTF-8"));
		
		// Convert the fields to behemoth annotations
		List<Annotation> annotations = fieldsToAnnotations(fields);
		doc.setAnnotations(annotations);
		writer.append(key, doc);
	}

	
	/**
	 * close the behemoth sequence file
	 * @throws IOException
	 */
	public void close() throws IOException {
		writer.syncFs();
		writer.close();
		isClosed = true;
	}
	
	/**
	 * @param key
	 *   The name of the field, mapped internally to annotations_<key>_value
	 * @param value
	 *   The value to assign to this key
	 */
	private Annotation createAnnotation(String key, String value) {
		Annotation annot = new Annotation();
		annot.setStart(-1);
		annot.setEnd(-1);
		annot.setType(key);
		Map<String, String> feature = new HashMap<String, String>();
		feature.put("value", value);
		annot.setFeatures(feature);
		return annot;
	}

	/**
	 * Convert the map of fields to a list of annotations on the behemoth doc
	 * 
	 * @param fields
	 *        Map of fields to their values. For multivalue add multiple
	 *        values to the List
	 * @return List of behemoth annotations to attach to the behemoth doc
	 */
	private List<Annotation> fieldsToAnnotations(
			Map<String, ? extends Collection<String>> fields) {
		List<Annotation> annotations = new ArrayList<Annotation>();

		for (Map.Entry<String, ? extends Collection<String>> entry : fields.entrySet()) {
			//System.out.format("%s -> %s\n", entry.getKey(),  entry.getValue());
			for (String value : entry.getValue()) {
				Annotation annot = createAnnotation(entry.getKey(), value);
				annotations.add(annot);
			}
		}

		return annotations;
	}
}
