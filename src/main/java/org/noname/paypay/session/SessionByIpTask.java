package org.noname.paypay.session;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SessionByIpTask {
    private static Logger logger = LoggerFactory.getLogger(SessionByIpTask.class);

    public static class PageCountMapper extends Mapper<Object, Text, Text, Text> {
        /***
         *  handle a line log unit in mapper.
         *  extract ip, timestamp, url
         *  output key = ip, value = JSON_String({timestamp: millisecond, url: string})
         */
        @SuppressWarnings("unchecked")
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            int first_end = input.indexOf("\"");
            String firstPart = input.substring(0, first_end);
            String[] components = firstPart.split(" ");
            JSONObject jsonObject = new JSONObject();
            try {
                // parse timestamp format yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'
                Date timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").parse(components[0]);
                // parse ip format ip:port
                String ip = components[2].substring(0, components[2].indexOf(':'));
                int url_end = input.indexOf("\"", first_end + 1);
                // parse request URL
                String url = input.substring(first_end, url_end).split(" ")[1];
                jsonObject.put("timestamp", timestamp.getTime());
                jsonObject.put("url", url);
                Text text = new Text();
                text.set(jsonObject.toJSONString());
                // {ip, JSON_String({timestamp: time_millisecond, url: url})}
                context.write(new Text(ip), text);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
    public static class PageCountReducer extends Reducer<Text,Text,Text,Text> {
        /***
         * 15 minutes per session, collect start_timestamp(min), end_timestamp(max), unique urls set
         * output key = ip, value = JSON_String({start_time: millisecond, end_time: millisecond, pages: unique_url_set})
         */
        @SuppressWarnings("unchecked")
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // a map with {timestamp, url}
            Map<Long, Set<String>> timeUrlMap = new HashMap<>();
            for (Text text: values) {
                try {
                    String value = text.toString();
                    JSONObject jsonObject = (JSONObject)new JSONParser().parse(value);
                    long timestamp = (Long)(jsonObject.get("timestamp"));
                    String url = jsonObject.get("url").toString();
                    if (timeUrlMap.containsKey(timestamp)) {
                        timeUrlMap.get(timestamp).add(url);
                    } else {
                        Set<String> urls = new HashSet<>();
                        urls.add(url);
                        timeUrlMap.put(timestamp, urls);
                    }
                } catch (org.json.simple.parser.ParseException e) {
                    logger.info(text.toString());
                    e.printStackTrace();
                }
            }
            List<Long> timestamps = new ArrayList<>(timeUrlMap.keySet());
            // sort timestamp in ascending order
            Collections.sort(timestamps);
            long max_timestamp = 0;
            Set<String> session_urls = new HashSet<>();
            long start = 0;
            long end = 0;
            // every 15 minutes with a session
            for (long timestamp: timestamps) {
                if (timestamp > max_timestamp) {
                    if (!session_urls.isEmpty()) {
                        JSONObject session_json = new JSONObject();
                        session_json.put("start_time", start);
                        session_json.put("end_time", end);
                        JSONArray json_urls = new JSONArray();
                        json_urls.addAll(session_urls);
                        session_json.put("pages", json_urls);
                        context.write(key, new Text(session_json.toJSONString()));
                    }
                    session_urls.clear();
                    max_timestamp = timestamp + 15 * 60 * 1000;
                    start = timestamp;
                } else {
                    session_urls.addAll(timeUrlMap.get(timestamp));
                    end = timestamp;
                }
                timeUrlMap.remove(timestamp);
            }
            if (!session_urls.isEmpty()) {
                JSONObject session_json = new JSONObject();
                session_json.put("start_time", start);
                session_json.put("end_time", end);
                JSONArray json_urls = new JSONArray();
                json_urls.addAll(session_urls);
                session_json.put("pages", json_urls);
                context.write(key, new Text(session_json.toJSONString()));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Session");
        job.setJarByClass(SessionByIpTask.class);
        job.setMapperClass(PageCountMapper.class);
        job.setReducerClass(PageCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
