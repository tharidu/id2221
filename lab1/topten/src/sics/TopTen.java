package sics;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTen {

  public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
  // Stores a map of user reputation to the record
  private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Map<String, String> parsed = XmlUtils.transformXmlToMap(value.toString());
    // String userId = parsed.get("AccountId");
    String reputation = parsed.get("Reputation");

    if (reputation == null) return;

    // Add this record to our map with the reputation as the key
    repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

    // If we have more than ten records, remove the one with the lowest reputation.
    if (repToRecordMap.size() > 10) {
      repToRecordMap.remove(repToRecordMap.firstKey());
    }
  }

  protected void cleanup(Context context) throws IOException, InterruptedException {
    // Output our ten records to the reducers with a null key
    for (Text t : repToRecordMap.values()) {
      context.write(NullWritable.get(), t);
    }
  }
}

public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
// Stores a map of user reputation to the record
// Overloads the comparator to order the reputations in descending order
private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
  for (Text value : values) {
    Map<String, String> parsed = XmlUtils.transformXmlToMap(value.toString());
    repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")), new Text(value));

    // If we have more than ten records, remove the one with the lowest reputation
    if (repToRecordMap.size() > 10) {
      repToRecordMap.remove(repToRecordMap.firstKey());
    }
  }

  for (Text t : repToRecordMap.descendingMap().values()) {
  // Output our ten records to the file system with a null key
    context.write(NullWritable.get(), t);
  }
}
}

public static class XmlUtils {
  // This helper function parses the stackoverflow into a Map for us.
   public static Map<String, String> transformXmlToMap(String xml) {
     Map<String, String> map = new HashMap<String, String>();
     try {
       // exploit the fact that splitting on double quote
       // tokenizes the data nicely for us
       String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
       for (int i = 0; i < tokens.length - 1; i += 2) {
         String key = tokens[i].trim();
         String val = tokens[i + 1];
         map.put(key.substring(0, key.length() - 1), val);
       }
     } catch (StringIndexOutOfBoundsException e) {
       System.err.println(xml);
     }
     return map;
   }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "top ten");
    job.setJarByClass(TopTen.class);

    job.setMapperClass(TopTenMapper.class);

    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(TopTenReducer.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
