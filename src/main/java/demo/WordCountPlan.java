package demo;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;

import java.util.Arrays;

public class WordCountPlan {

    public static void main(String[] args){

        /* Get a plan builder */
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(WordCountPlan.class);

        /* Start building the Apache WayangPlan */
        WayangPlan plan = planBuilder
                /* Read the text file */
                .readTextFile("file:/tmp/test.txt").withName("Load file")

                /* Split each line by non-word characters */
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withName("Split words")

                /* Filter empty tokens */
                .filter(token -> !token.isEmpty())
                .withName("Filter empty words")

                /* Attach counter to each word */
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
                .withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")


                /* Build the plan. This is for now hardcoded to have  stdout localsink, which should be resolved */
                .dataQuanta().getPlanBuilder().build();




        // Execute the plan now, prints to stdout.
        wayangContext.execute("WordCount", plan);

    }


}

