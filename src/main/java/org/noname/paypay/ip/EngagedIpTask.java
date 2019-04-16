package org.noname.paypay.ip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.noname.paypay.session.SessionByIpTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EngagedIpTask {
    private static Logger logger = LoggerFactory.getLogger(EngagedIpTask.class);

    /**
     * extract "start_time", "end_time", and "ip" from a line from a file
     * output key=(end_time - start_time), value = ip
     */
    public static class SessionIpMapper extends Mapper<Object, Text, LongWritable, Text> {
        @SuppressWarnings("unchecked")
        @Override
        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            String value = text.toString();
            int tabPos = value.indexOf("\t");
            String ip = value.substring(0, tabPos);
            String json_string = value.substring(tabPos + 1);
            try {
                JSONObject jsonObject = (JSONObject) new JSONParser().parse(json_string);
                long start_time = (long)jsonObject.get("start_time");
                long end_time = (long)jsonObject.get("end_time");
                context.write(new LongWritable(end_time - start_time), new Text(ip));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * collect all ips with the same long time duration
     */
    public static class SessionIpReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
        @SuppressWarnings("unchecked")
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONArray jsonArray = new JSONArray();
            for (Text text: values) {
                jsonArray.add(text.toString());
            }
            context.write(key, new Text(jsonArray.toJSONString()));
        }
    }

    /**
     * reverse key (time duration)
     */
    public static class ReverseKeyComparator extends WritableComparator {
        protected ReverseKeyComparator() {
            super(LongWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            LongWritable value1 = (LongWritable)w1;
            LongWritable value2 = (LongWritable)w2;
            return (-1) * value1.compareTo(value2);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EngagedIp");
        job.setJarByClass(SessionByIpTask.class);
        job.setMapperClass(SessionIpMapper.class);
        job.setReducerClass(SessionIpReducer.class);
        job.setSortComparatorClass(ReverseKeyComparator.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
