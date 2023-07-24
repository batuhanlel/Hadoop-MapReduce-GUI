package com.lel.bigdatatermproject;

import com.lel.bigdatatermproject.fifth.FifthMapperClass;
import com.lel.bigdatatermproject.fifth.FifthReducerClass;
import com.lel.bigdatatermproject.first.FirstMapperClass;
import com.lel.bigdatatermproject.first.FirstReducerClass;
import com.lel.bigdatatermproject.fourth.FourthReducerClass;
import com.lel.bigdatatermproject.fourth.FourthMapperClass;
import com.lel.bigdatatermproject.second.SecondMapperClass;
import com.lel.bigdatatermproject.second.SecondReducerClass;
import com.lel.bigdatatermproject.third.ThirdMapperClass;
import com.lel.bigdatatermproject.third.ThirdReducerClass;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Runner {

    JobClient jobClient = new JobClient();
    JobConf jobConf = new JobConf(Runner.class);

    public Runner() {
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
    }

    public String runFirstTask(String inputPath) {
        String outputPath = "/output/first/" + getFormattedLocalDateTime();

        jobConf.setJobName("First Task");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(FirstMapperClass.class);
        jobConf.setReducerClass(FirstReducerClass.class);

        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        jobClient.setConf(jobConf);
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder result = new StringBuilder("Task Completed Successfully.\nOutput file located in ");
        result
                .append(outputPath)
                .append("\n\nOutput File Contents :\n")
                .append(getOutputFileContentsFromHDFS(outputPath));

        return result.toString();
    }

    public String runSecondTask(String inputPath) {
        String outputPath = "/output/second/" + getFormattedLocalDateTime();

        jobConf.setJobName("Second Task");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(DoubleWritable.class);

        jobConf.setMapperClass(SecondMapperClass.class);
        jobConf.setReducerClass(SecondReducerClass.class);

        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        jobClient.setConf(jobConf);
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder result = new StringBuilder("Task Completed Successfully.\nOutput file located in ");
        result
                .append(outputPath)
                .append("\n\nOutput File Contents :\n")
                .append(getOutputFileContentsFromHDFS(outputPath));

        return result.toString();
    }

    public String runThirdTask(String inputPath) {
        String outputPath = "/output/third/" + getFormattedLocalDateTime();

        jobConf.setJobName("Third Task");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(DoubleWritable.class);

        jobConf.setMapperClass(ThirdMapperClass.class);
        jobConf.setReducerClass(ThirdReducerClass.class);

        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        jobClient.setConf(jobConf);
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder result = new StringBuilder("Task Completed Successfully.\nOutput file located in ");
        result
                .append(outputPath)
                .append("\n\nOutput File Contents :\n")
                .append(getOutputFileContentsFromHDFS(outputPath));

        return result.toString();
    }

    public String runFourthTask(String inputPath) {
        String outputPath = "/output/fourth/" + getFormattedLocalDateTime();

        jobConf.setJobName("Fourth Task");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(DoubleWritable.class);

        jobConf.setMapperClass(FourthMapperClass.class);
        jobConf.setReducerClass(FourthReducerClass.class);

        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        jobClient.setConf(jobConf);
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder result = new StringBuilder("Task Completed Successfully.\nOutput file located in ");
        result
                .append(outputPath)
                .append("\n\nOutput File Contents :\n")
                .append(getOutputFileContentsFromHDFS(outputPath));

        return result.toString();
    }

    public String runFifthTask(String inputPath) {
        String outputPath = "/output/fifth/" + getFormattedLocalDateTime();

        jobConf.setJobName("Fifth Task");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(FifthMapperClass.class);
        jobConf.setReducerClass(FifthReducerClass.class);

        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        jobClient.setConf(jobConf);
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder result = new StringBuilder("Task Completed Successfully.\nOutput file located in ");
        result
                .append(outputPath)
                .append("\n\nOutput File Contents :\n")
                .append(getOutputFileContentsFromHDFS(outputPath));

        return result.toString();
    }

    private String getFormattedLocalDateTime() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd__HHmmss");

        return now.format(formatter);
    }

    private String getOutputFileContentsFromHDFS(String outputPathString) {
        StringBuilder contents = new StringBuilder();

        try {
            Configuration config = new Configuration();
            config.set("fs.defaultFS", "hdfs://localhost:9000");

            FileSystem fileSystem = FileSystem.get(config);

            Path outputPath = new Path(outputPathString + "/part-00000");
            fileSystem.access(outputPath, FsAction.READ);

            FSDataInputStream inputStream = fileSystem.open(outputPath);

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                contents.append(line).append(System.lineSeparator());
            }

            reader.close();
            inputStream.close();
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return contents.toString();
    }

}
