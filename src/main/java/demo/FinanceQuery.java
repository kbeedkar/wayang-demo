package demo;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.java.Java;

import java.util.Collection;

public class FinanceQuery {
    public static void main(String[] args){

        /* Get a plan builder */
        WayangContext context = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());
                //.withPlugin(Spark.basicPlugin());



        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("FinanceQuery")
                .withUdfJarOf(FinanceQuery.class);

        /* Start building the Apache WayangPlan */
        Collection<Record> output = planBuilder
                /* Read the text file */
                .readTextFile("file:/Users/kbeedkar/Documents/Projects/wayang/wayang-test/src/main/resources/input/Stock_Prices_2022.csv")



                /* Append a field with value '1' to the record*/
                .map(line -> {
                    String [] vals = line.split("\\,");
                    return new Record(vals[0], vals [1], Double.parseDouble(vals[2]), 1);
                })

                /* Filter for year 2022 */
                .filter(record -> {
                    //System.out.println("Filtering..");
                    return ((String)record.getField(0)).contains("2022");
                })

                /* Aggregate */
                .reduceByKey(record -> record.getField(1), (r1, r2) -> {
                    return new Record(r1.getField(0), r1.getField(1), r1.getDouble(2) + r2.getDouble(2), r1.getInt(3) + r2.getInt(3));
                })
                /* Average */
                .map(r -> new Record(r.getField(1), r.getDouble(2)/r.getInt(3)))
                .collect();


        /* Print out the results */
        System.out.println(output);


    }
}
