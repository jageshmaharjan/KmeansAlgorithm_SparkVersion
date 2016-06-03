package com.thu;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jagesh on 05/14/2016.
 */
public class KMeanClustering
{
    public static final String FS = System.getProperty("file.separator");
    public static int cols = 41;    //Matrix Dimension for no.s of Columns
    public static int k = 5;        //cluster size K (2 or 5)
    public static final String INPUT_PATH = "input" + FS + "kddcup.testdata.unlabeled";
    public static final String OUTPUT_PATH = "output";

    public static void main(String[] args) throws IOException
    {
//        String dataFile = args[0];//"C:\\Users\\jagesh\\IdeaProjects\\KMeanSpark\\input\\kddcup.testdata.unlabeled_10_percent";
//        String dataFile  = args[1];//"C:\\Users\\jagesh\\IdeaProjects\\KMeanSpark\\input\\kddcup.testdata.unlabeled";
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("KMeans Algorithm").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //reads each line as a string, from the file
        JavaRDD<String> allRecords = sc.textFile(INPUT_PATH);

        //removes all the duplicated records
        JavaRDD<String> uniqueRecords = allRecords.distinct();

        //String to  float conversion and Normalization on points
        JavaRDD<MyObject> myObjectsRDD = uniqueRecords.map(line -> new MyObject(line)).cache();

        //random selection of k centroid points
        List<MyObject> centroids = myObjectsRDD.takeSample(false,k);

        //assigining the centroid id for the centroids
        for (int i=0; i<centroids.size(); i++)
        {
//            System.out.println(centroids.get(i));
            centroids.get(i).setClusterID(i);
        }

        List<MyObject> oldCentroids = new ArrayList<>();

        int iterations = 0;
        while (!centroids.equals(oldCentroids))
        {
            double t1 = System.currentTimeMillis();
            // Saving state of the old centroids for later comparision
            oldCentroids.clear();
            for (MyObject centroid : centroids)
            {
                oldCentroids.add(new MyObject(centroid));
            }

            //addigining the clusterID to the data Points based on the Euclidian Distance
            myObjectsRDD = myObjectsRDD.map(point -> new MyObject(point, centroids)).cache();

//            System.out.println("cluster1: " + myObjectsRDD.filter(point -> point.getClusterID()==0).count());
//            System.out.println("cluster2: " + myObjectsRDD.filter(point -> point.getClusterID()==1).count());

            for (MyObject centroid : centroids)
            {
                JavaRDD<MyObject> clusters = myObjectsRDD.filter(point -> point.getClusterID() == centroid.getClusterID());
//                System.out.println(clusters.count());
                MyObject reduce = clusters.reduce(MyObject::new);
                centroid.changePosition(reduce,clusters.count());
                System.out.println(centroid + " count is " + clusters.count());
            }
            System.out.println("iterations: "+iterations);
            ++iterations;
//            myObjectsRDD.saveAsTextFile("C:\\Users\\jagesh\\IdeaProjects\\KMeanSpark\\output");
            double t2 = System.currentTimeMillis();
            double t = t2-t1;
            System.out.println(t/1000 +" second/s" );
        }

        JavaRDD<String> saveToFile = myObjectsRDD.map(line -> line.toString());
        FileUtils.deleteDirectory(new File(OUTPUT_PATH));
        saveToFile.saveAsTextFile(OUTPUT_PATH);//("C:\\Users\\jagesh\\IdeaProjects\\KMeanSpark\\output");
    }
}
