package org.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by r.benrejeb on 25/11/2014.
 */
public class SumTreesHeight {
SparkConf conf;
JavaSparkContext sc;
    public SumTreesHeight() {
         conf = new SparkConf()
                .setAppName("arbres-alignement")
                .setMaster("local");
         sc = new JavaSparkContext(conf);
    }

    public Float sum(String fileName){
        return sc.textFile(fileName).filter(line -> !line.startsWith("geom"))
                .map(line -> line.split(";")).filter(data -> data.length>3)
                .map(fields -> Float.parseFloat(fields[3]))
                .reduce((x,y)->x+y);
    }

    public long doCount(String fileName){

        return sc.textFile(fileName).filter(line -> !line.startsWith("geom")&&line.length()>0)
                .map(line -> line.split(";"))
                .map(fields -> Float.parseFloat(fields[3]))
                .filter(height -> height > 0).map(item -> 1).reduce((x,y)->x+y)
                ;

    }
}
