/**
 * Created by Data Pirates
 **/

package dhb.dhb;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import jdk.nashorn.internal.ir.WhileNode;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import scala.Tuple2;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

import org.apache.spark.streaming.Time;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import com.amazonaws.services.kinesis.model.PutRecordRequest;


//import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import java.util.Date;
import java.text.SimpleDateFormat;


public class App 
{
	private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        System.out.println("Hello World");

        String kinesisAppName = "hello_spark_kinesis";
        String streamName = "log_stream";
        String endpointUrl = "kinesis.us-west-2.amazonaws.com";

        AmazonKinesisClient kinesisClient =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        kinesisClient.setEndpoint(endpointUrl);
        int numShards =
                kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();

        int numStreams = numShards;

        System.out.println(kinesisClient.describeStream(streamName));

        // Spark Streaming batch interval
        Duration batchInterval = new Duration(5000);

        // Kinesis checkpoint interval.  Same as batchInterval for this example.
        Duration kinesisCheckpointInterval = batchInterval;

        //String regionName = KinesisExampleUtils.getRegionNameByEndpoint(endpointUrl);
        String regionName = "us-west-2";

        SparkConf sparkConfig = new SparkConf().setMaster("local[8]").setAppName("Real-time-spark");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

        // Create the Kinesis DStreams
        List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            System.out.println("Num Stream 1");
            streamsList.add(
                    KinesisUtils.createStream(jssc, kinesisAppName, streamName, endpointUrl, regionName,
                            InitialPositionInStream.LATEST, kinesisCheckpointInterval,
                            StorageLevel.MEMORY_AND_DISK_2())
            );
        }

        // Union all the streams if there is more than 1 stream
        JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            System.out.println("US 1");
            unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            // Otherwise, just use the 1 stream
            System.out.println("US 2");
            unionStreams = streamsList.get(0);
        }

        // Convert each line of Array[Byte] to String, and split into words

        JavaDStream<String> words = unionStreams.map( (byte[] line) -> {
                    String s = new String(line, StandardCharsets.UTF_8);
                    String[] splited = s.split("\\s+");
                    String retS = "LOG_ERR";
                    try{
                        retS = splited[0] + ":" + splited[10] + splited[6];
                    } catch (ArrayIndexOutOfBoundsException e){
                        retS = retS + e.getMessage();
                    }

                    return retS;
                }
        );
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair( (String s) -> {
                    return new Tuple2<String, Integer>(s, 1);
                }
        ).reduceByKey( (Integer i1, Integer i2) -> {
                    return i1+i2;
                }
        );

        // Print the first 10 wordCounts
        System.out.println("Printing Words - - - - - - - - - - -");
        //words.print();

        wordCounts.foreachRDD((JavaPairRDD<String, Integer> rdd) -> {
            rdd.foreachPartition((Iterator<Tuple2<String, Integer>> p ) -> {
                AmazonKinesisClient rtClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
                rtClient.setEndpoint(endpointUrl);

                while(p.hasNext()){
                    Tuple2 t = p.next();
                    String thing = t._1().toString();
                    int count = (int) t._2();
                    String data = "";
                    if(thing.contains("LOG_ERR")){
                        continue;
                    }

                    if(count > 5){
                        if(count > 10){
                            // RED
                            thing = thing.replace("\"", "_");
                            thing = thing.replace(":", "-");
                            String ip = thing.split("-")[0];
                            thing = thing.split("-")[2];
                            String timeStamp = new SimpleDateFormat("yyyy:MM:dd HH.mm.ss").format(new Date());
                            data = "{\"code\":\"11\"" +", \"ip\":\"" + ip +"\"" + ",\"timestamp\":\"" + timeStamp + "\"" + ",\"thing\":\"" + thing + "\", \"count\":\"" + count + "\"}";
                        } else {
                            thing = thing.replace("\"", "_");
                            thing = thing.replace(":", "-");
                            String ip = thing.split("-")[0];
                            thing = thing.split("-")[2];
                            String timeStamp = new SimpleDateFormat("yyyy:MM:dd HH.mm.ss").format(new Date());
                            data = "{\"code\":\"10\"" +", \"ip\":\"" + ip +"\"" + ",\"timestamp\":\"" + timeStamp + "\"" + ",\"thing\":\"" + thing + "\", \"count\":\"" + count + "\"}";
                        }

                        PutRecordRequest putRecordRequest = new PutRecordRequest().withStreamName("rt_ks")
                                .withPartitionKey("d-h-b")
                                .withData(ByteBuffer.wrap(data.getBytes()));

                        // Put record into the DeliveryStream
                        rtClient.putRecord(putRecordRequest);
                    }
                }
            });
        });

        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();

    }
}
