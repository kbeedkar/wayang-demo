package demo;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;


import java.util.Arrays;
import java.util.Collection;

public class Northwind {


    public static void main (String[] args) {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/northwind");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");


        /* Create a wayang context */
        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                ;

        // Get a plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Demo")
                .withUdfJarOf(Northwind.class);



        // Start building a plan
        //For each order, calculate a subtotal for each Order (identified by OrderID)
        Collection<Record> output = planBuilder

                // Extract data from postgres
                .readTable(new PostgresTableSource("order_details"))

                // Get order_id, unit_price, quantity, and discount
                .map(record -> new Record(
                        record.getInt(0),
                        record.getDouble(2),
                        record.getInt(3),
                        record.getDouble(4)
                        )

                )

                // Compute subtotal as unit_price * quantity * (1 - discount)
                .map(record -> new Record(
                    record.getInt(0),
                    record.getDouble(1) * record.getInt(2) * (1 - record.getDouble(3)))
                )
                .withTargetPlatform(Spark.platform())

                // Group by order id and compute subtotals
                .reduceByKey(record -> record.getField(0),
                        (r1, r2) -> new Record(
                                r1.getField(0),
                            r1.getDouble(1) + r2.getDouble(1))

                        )

                // Sort by order id
                .sort(record -> record.getField(0)
                )
                .collect();

        // helper to print
        printRecords(output);

    }



    private static void printRecords(Collection<Record> output) {
        for(Record record : output) {
            System.out.println(record.getField(0) + " | " + record.getField(1));
        }


    }


}

