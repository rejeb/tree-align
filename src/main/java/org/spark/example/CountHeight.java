package org.spark.example;

import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.mutable.ArrayLike;

/**
 * Created by r.benrejeb on 18/11/2014.
 */
public class CountHeight {

    public static void main(String[] args){


        SparkConf conf = new SparkConf()
                .setAppName("arbres-alignement")
                .setMaster("local");
      JavaSparkContext  sc = new JavaSparkContext(conf);
      JavaRDD<Float> cache=sc.textFile("arbresalignementparis2010.csv").filter(line -> !line.startsWith("geom"))
                .map(line -> line.split(";")).filter(data -> data.length > 3)
                .map(fields -> Float.parseFloat(fields[3])).cache();

        long count=cache.filter(x->x>0).map(x->1).reduce((x,y)->x+y);
        float sum=cache.reduce((x,y)->x+y);
        System.out.println("Count :"+count);
        System.out.println("Sum :"+sum);
        System.out.println("Average : "+(sum/count));
      
        
    }


}
