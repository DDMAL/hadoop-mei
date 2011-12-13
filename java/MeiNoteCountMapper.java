package org.myorg;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import nu.xom.*;

/*
 * MeiNoteCountMapper is the class used for the map function.
 * This class uses the XOM library to do its .xml processing.
 * 
 */
public class MeiNoteCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	//the first two generic types indicate what are the types of the <key,value> pairs coming into this class
	//and the last two generic represent what the type of the <key,value> pairs this class will output to the reducer 
	//(or combiner if you have one)

	private final static IntWritable one = new IntWritable(1);

	@Override
	/*
	 * The map() method is called once for each input taken from the input path. This
	 * method will be receiving a string representation of the entire .xml file which
	 * is then converted into a tree using the XOM library. The purpose of this method
	 * is to get all the "note" elements in the .mei file and retrieve their note "dur"
	 * duration attributes. The output of this method will be of the form key:noteduration
	 * value:1, which is then sent for local aggregation by the combiner, which will local
	 * aggregation of results for each map() call.
	 * 
	 * note that the two first arguments are of the same type as the first two generics of the class def
	 * -> they need to match
	 * 
	 */
	public void map(LongWritable key, Text value1, Context context)
			throws IOException, InterruptedException {

		String xmlString = value1.toString();

		Builder parser = null;
		Document doc = null;
		Element root = null;
		try {
			parser = new Builder();
			doc = parser.build(xmlString, null);//builds a tree
			root = doc.getRootElement();//get root of the tree

			ArrayList<Element> notes = getElementsWithName(root, "note");

			for (Element e : notes) {
				if(e.getAttributeValue("dur")!=null){
					context.write(new Text(e.getAttributeValue("dur")), one);//input for the combiner/reducer
				}
			}

		} catch (ParsingException ex) {
			System.err.println("Parsing Exception");
		} catch (IOException ex) {
			System.err.println("IO Exception");
		}

	}

	/*
	 * Simple method used for retrieving specified elements.
	 * 
	 */
	private static ArrayList<Element> getElementsWithName(Element startElem,
			String elementName) {
		ArrayList<Element> retList = new ArrayList<Element>();

		getElementsWithNameHelper(startElem, elementName, retList);

		return retList;
	}

	/*
	 * Recursive helper for the previous method.
	 * 
	 */
	private static void getElementsWithNameHelper(Element current,
			String elementName, ArrayList<Element> retList) {

		if (current.getQualifiedName().equals(elementName)) {
			retList.add(current);
		}
		for (int i = 0; i < current.getChildElements().size(); i++) {
			getElementsWithNameHelper(current.getChildElements().get(i),
					elementName, retList);
		}
	}

}