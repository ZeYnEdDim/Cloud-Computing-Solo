package it.unipi.solo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WikimediaPageviewAnalytics {

    public static class PageviewRecord implements Writable {
        private String project;
        private String page;
        private String hour;
        private long views;

        public PageviewRecord() {
            this.project = "";
            this.page = "";
            this.hour = "";
            this.views = 0L;
        }

        public PageviewRecord(String project, String page, String hour, long views) {
            this.project = project;
            this.page = page;
            this.hour = hour;
            this.views = views;
        }

        public String getProject() { return project; }
        public String getPage() { return page; }
        public String getHour() { return hour; }
        public long getViews() { return views; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(project);
            out.writeUTF(page);
            out.writeUTF(hour);
            out.writeLong(views);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.project = in.readUTF();
            this.page = in.readUTF();
            this.hour = in.readUTF();
            this.views = in.readLong();
        }
    }

    // JOB 1: ETL PIPELINE
    public static class ETLMapper extends Mapper<LongWritable, Text, NullWritable, PageviewRecord> {
        private String inputHour;

        @Override
        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            inputHour = extractHour(split.getPath().getName());
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split(" ", 4);
            if (parts.length != 4) {
                return;
            }

            String project = parts[0];
            String page = parts[1];
            long views = parseLong(parts[2]);

            if (project.isEmpty() || page.isEmpty() || views <= 0) {
                return;
            }

            context.write(NullWritable.get(), new PageviewRecord(project, page, inputHour, views));
        }

        private static long parseLong(String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }

        private static String extractHour(String filename) {
            if (filename.startsWith("pageviews-") && filename.length() >= 21) {
                return filename.substring(10, 18) + "-" + filename.substring(19, 21);
            }
            return "unknown";
        }
    }

    // JOB 2: ANALYTICS
    public static class AnalyticsMapper extends Mapper<NullWritable, PageviewRecord, Text, LongWritable> {
        private Map<String, Long> metricsCache;

        @Override
        protected void setup(Context context) {
            metricsCache = new HashMap<>();
        }

        @Override
        public void map(NullWritable key, PageviewRecord record, Context context) {
            addMetric("TOTAL_VIEWS", record.getViews());
            addMetric("PROJECT:" + record.getProject(), record.getViews());
            addMetric("PAGE:" + record.getProject() + "|" + record.getPage(), record.getViews());
            addMetric("HOUR:" + record.getHour(), record.getViews());
        }

        private void addMetric(String key, long value) {
            metricsCache.put(key, metricsCache.getOrDefault(key, 0L) + value);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text outKey = new Text();
            LongWritable outValue = new LongWritable();
            for (Map.Entry<String, Long> entry : metricsCache.entrySet()) {
                outKey.set(entry.getKey());
                outValue.set(entry.getValue());
                context.write(outKey, outValue);
            }
        }
    }

    public static class MetricsPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            if (numPartitions <= 1) {
                return 0;
            }
            String keyString = key.toString();
            String prefix = keyString.contains(":") ? keyString.split(":")[0] : keyString;
            return (prefix.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class AnalyticsReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
        private long totalViews = 0L;
        private Map<String, Long> projectViews = new HashMap<>();
        private Map<String, Long> pageViews = new HashMap<>();
        private Map<String, Long> hourViews = new HashMap<>();
        private int topN;

        @Override
        protected void setup(Context context) {
            topN = context.getConfiguration().getInt("analytics.top.n", 10);
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }

            String metric = key.toString();
            if (metric.equals("TOTAL_VIEWS")) {
                totalViews += sum;
            } else if (metric.startsWith("PROJECT:")) {
                String project = metric.substring("PROJECT:".length());
                projectViews.put(project, projectViews.getOrDefault(project, 0L) + sum);
            } else if (metric.startsWith("PAGE:")) {
                String page = metric.substring("PAGE:".length());
                addTopCandidate(pageViews, page, sum, topN);
            } else if (metric.startsWith("HOUR:")) {
                String hour = metric.substring("HOUR:".length());
                hourViews.put(hour, hourViews.getOrDefault(hour, 0L) + sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder json = new StringBuilder();
            json.append("{\n");
            json.append("  \"summary\": {\n");
            json.append("    \"total_views\": ").append(totalViews).append("\n");
            json.append("  },\n");
            json.append("  \"top_projects_by_views\": ").append(getTopNJson(projectViews, topN, "project", "views")).append(",\n");
            json.append("  \"top_pages_by_views\": ").append(getTopNJson(pageViews, topN, "page", "views")).append(",\n");
            json.append("  \"views_by_hour\": ").append(getSortedJson(hourViews, "hour", "views")).append("\n");
            json.append("}");

            context.write(NullWritable.get(), new Text(json.toString()));
        }

        private void addTopCandidate(Map<String, Long> map, String key, long value, int n) {
            if (map.containsKey(key)) {
                map.put(key, map.get(key) + value);
                return;
            }

            if (map.size() < n) {
                map.put(key, value);
                return;
            }

            String minKey = null;
            long minValue = Long.MAX_VALUE;
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                if (entry.getValue() < minValue) {
                    minKey = entry.getKey();
                    minValue = entry.getValue();
                }
            }

            if (minKey != null && value > minValue) {
                map.remove(minKey);
                map.put(key, value);
            }
        }
        private String getTopNJson(Map<String, Long> map, int n, String keyName, String valueName) {
            List<Map.Entry<String, Long>> entries = new ArrayList<>(map.entrySet());
            entries.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            StringBuilder sb = new StringBuilder("[\n");
            int limit = Math.min(n, entries.size());
            for (int i = 0; i < limit; i++) {
                Map.Entry<String, Long> entry = entries.get(i);
                sb.append("    {\"").append(keyName).append("\": \"").append(escape(entry.getKey()))
                  .append("\", \"").append(valueName).append("\": ").append(entry.getValue()).append("}");
                if (i < limit - 1) {
                    sb.append(",\n");
                }
            }
            sb.append("\n  ]");
            return sb.toString();
        }

        private String getSortedJson(Map<String, Long> map, String keyName, String valueName) {
            List<String> keys = new ArrayList<>(map.keySet());
            Collections.sort(keys);

            StringBuilder sb = new StringBuilder("[\n");
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                sb.append("    {\"").append(keyName).append("\": \"").append(escape(key))
                  .append("\", \"").append(valueName).append("\": ").append(map.get(key)).append("}");
                if (i < keys.size() - 1) {
                    sb.append(",\n");
                }
            }
            sb.append("\n  ]");
            return sb.toString();
        }

        private String escape(String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: WikimediaPageviewAnalytics <number_of_reducers> <input_path> <intermediate_path> <output_path> <top_n>");
            System.exit(-1);
        }

        int numReducers = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String intermediatePath = args[2];
        String outputPath = args[3];
        int topN = Integer.parseInt(args[4]);

        Configuration conf = new Configuration();
        conf.setInt("analytics.top.n", topN);
        conf.set("mapreduce.map.memory.mb", "1536");
        conf.set("mapreduce.map.java.opts", "-Xmx1024m");
        conf.set("mapreduce.reduce.memory.mb", "1536");
        conf.set("mapreduce.reduce.java.opts", "-Xmx1024m");

        FileSystem fs = FileSystem.get(conf);
        Path intermediate = new Path(intermediatePath);
        Path output = new Path(outputPath);
        if (fs.exists(intermediate)) {
            fs.delete(intermediate, true);
        }
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Job job1 = Job.getInstance(conf, "Job 1: Pageview ETL");
        job1.setJarByClass(WikimediaPageviewAnalytics.class);
        job1.setMapperClass(ETLMapper.class);
        job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(PageviewRecord.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, intermediate);

        if (!job1.waitForCompletion(true)) {
            System.err.println("ETL Job failed.");
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Job 2: Pageview Analytics");
        job2.setJarByClass(WikimediaPageviewAnalytics.class);
        job2.setMapperClass(AnalyticsMapper.class);
        job2.setPartitionerClass(MetricsPartitioner.class);
        job2.setReducerClass(AnalyticsReducer.class);
        job2.setNumReduceTasks(numReducers);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, intermediate);
        FileOutputFormat.setOutputPath(job2, output);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}


