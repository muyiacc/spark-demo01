package cc.seektao;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class JavaBaseRDD01 implements Serializable {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("create rdd");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // create rdd
        //createRDD(sc);

        // RDD 并行度与分区
        //rddNumSlices(sc);

        // RDD 转换算子
        // 单 value
        rddConversionOperatorSingleValue(sc);

        sc.stop();
    }

    private static void rddConversionOperatorSingleValue(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 3);

        /**
         * map
         * 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
         */
        System.out.println("==========map===========");
        rdd.map(value -> value + 1).foreach(System.out::println); // 打印所有元素
        System.out.println("============");
        rdd.map(value -> value + "").foreach(System.out::println);

        /**
         * mapPartitions
         * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
         * 理，哪怕是过滤数据
         */
        System.out.println("=========mapPartitions============");
        rdd.mapPartitions(values -> {
            List<Integer> filteredValues = new ArrayList<>();
            while (values.hasNext()) {
                Integer value = values.next();
                if (value > 2) {
                    filteredValues.add(value);
                }
            }
            return filteredValues.iterator();
        }).foreach(System.out::println);

        System.out.println("获取每个分区的最大值");
        JavaRDD<Integer> resultRDD = rdd.mapPartitions(partition -> {
            List<Integer> partitionList = new ArrayList<>();
            while (partition.hasNext()) {
                partitionList.add(partition.next());
            }

            if (partitionList.isEmpty()) {
                return Collections.emptyIterator();
            } else {
                Integer max = Collections.max(partitionList);
                return Collections.singletonList(max).iterator();
            }
        });

        List<Integer> result = resultRDD.collect();
        System.out.println("每个分区最大值：" + String.join(", ", result.stream().map(String::valueOf).toArray(String[]::new)));

        /**
         * mapPartitionsWithIndex
         * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
         * 理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。
         */
        System.out.println("=======mapPartitionsWithIndex=========");
        rdd.mapPartitionsWithIndex((index, values) -> {
            List<Tuple2<Integer, Integer>> indexedValues = new ArrayList<>();
            while (values.hasNext()) {
                Integer value = values.next();
                indexedValues.add(new Tuple2<>(index, value));
            }
            return indexedValues.iterator();
        }, true).foreach(System.out::println);

        System.out.println("获取第二个数据分区的数据");
        rdd.mapPartitionsWithIndex((index, values) -> {
            if (index == 2) {
                List<Tuple2<Integer, Integer>> indexedValues = new ArrayList<>();
                while (values.hasNext()) {
                    Integer value = values.next();
                    indexedValues.add(new Tuple2<>(index, value));
                }
                return indexedValues.iterator();
            } else {
                return Collections.emptyIterator();
            }
        }, true).foreach(System.out::println);

        /**
         * flatMap
         * 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
         */
        System.out.println("==========flatMap===========");
        JavaRDD<List<Integer>> flatMapRDD = sc.parallelize(Arrays.asList(
                Arrays.asList(1, 2), Arrays.asList(3, 4)
        ));
        flatMapRDD.flatMap(List::iterator).foreach(System.out::println);

        System.out.println("将 List(List(1,2),3,List(4,5))进行扁平化操作");
        List<Object> mixedList = Arrays.asList(
                Arrays.asList(1, 2),
                3,
                Arrays.asList(3, 5)
        );

        JavaRDD<Object> flatMapRDD2 = sc.parallelize(mixedList);

        JavaRDD<Object> flattenedRDD = flatMapRDD2.flatMap(item -> {
            List<Object> resultList = new ArrayList<>();
            if (item instanceof List) {
                resultList.addAll((List<?>) item);
            } else {
                resultList.add(item);
            }
            return resultList.iterator();
        });

        flattenedRDD.foreach(System.out::println);


        /**
         * glom
         * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
         */
        JavaRDD<Integer> glomRDD = sc.parallelize(
                Arrays.asList(1, 2, 3, 4),
                1
        );

        JavaRDD<List<Integer>> glomRDD2 = glomRDD.glom();
        System.out.println("计算所有分区最大值求和（分区内取最大值，分区间最大值求和）");

        JavaRDD<Integer> glomRDD3 = sc.parallelize(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9),
                3
        );

        JavaRDD<Integer> maxValueRDD = glomRDD3.glom().map(partitionArray -> {
            if (partitionArray.isEmpty()) {
                return 0;
            } else {
                return Collections.max(partitionArray);
            }
        });

        List<Integer> maxValues = maxValueRDD.collect();
        System.out.println("每个分区最大值：" + String.join(", ", maxValues.stream().map(String::valueOf).toArray(String[]::new)));
        System.out.println("每个分区最大值的和：" + maxValueRDD.reduce(Integer::sum));

    }

    private static void rddNumSlices(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(1, 2, 3),
                2 // 并行度
        );
        rdd.collect().forEach(System.out::println);

        JavaRDD<Integer> rdd2 = sc.parallelize(
                Arrays.asList(4, 5, 6),
                2 // 并行度
        );
        rdd2.foreach(System.out::println);
    }

    private static void createRDD(JavaSparkContext sc) {
        // method 1
        JavaRDD<Integer> rdd1 = sc.parallelize(
                Arrays.asList(1, 2, 3)
        );
        // method 2
        JavaRDD<Integer> rdd2 = sc.parallelize(
                Arrays.asList(4, 5, 6)
        );
        rdd1.collect().forEach(System.out::println);
        rdd2.collect().forEach(System.out::println);

        // method 3
        JavaRDD<String> rdd3 = sc.textFile("data/input.txt");
        rdd3.collect().forEach(System.out::println);
    }
}
