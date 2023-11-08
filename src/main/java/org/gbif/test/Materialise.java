/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.test;

import java.io.Serializable;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import lombok.Builder;

import static org.apache.spark.sql.functions.*;

@Builder(toBuilder = true)
public class Materialise implements Serializable {
  private final String source;
  private final String target;

  public static void main(String[] args) throws InterruptedException {
    Materialise.builder().source("/tmp/tree.csv").target("/tmp/tree-materialised").build().run();
  }

  public void run() throws InterruptedException {
    SparkSession spark =
        SparkSession.builder().appName("Materialise path").enableHiveSupport().getOrCreate();

    spark.sql("use tim");
    spark.sparkContext().conf().set("hive.exec.compress.output", "true");
    AppendArray.register("array_append", spark);

    // Read input file adding an empty lineage column
    Dataset<Row> input =
        spark
            .read()
            .option("header", false)
            .schema(
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField("id", DataTypes.StringType, false),
                      DataTypes.createStructField("parentID", DataTypes.StringType, true),
                      DataTypes.createStructField("rank", DataTypes.StringType, false),
                      DataTypes.createStructField("name", DataTypes.StringType, false)
                    }))
            .csv(source)
            .withColumn(
                "path", functions.lit(null).cast(DataTypes.createArrayType(DataTypes.StringType)))
            .filter(not(col("id").startsWith("201891")));

    spark.sql("DROP TABLE IF EXISTS tree_0");
    input.write().format("parquet").saveAsTable("tree_0");

    // A breadth first traversal of the tree follows.
    // Loop with a self join, copying down the parent lineage each time and rewriting the full table
    // into a new table. With sufficient looping, the lineage propagates to the leaf nodes.
    int maxSize = Integer.MAX_VALUE;
    int loop = 0;
    while (loop <= maxSize) {
      Dataset<Row> df =
          spark.sql(
              String.format(
                  "SELECT "
                      + "  c.id AS id, "
                      + "  c.parentID AS parentID, "
                      + "  c.rank AS rank, "
                      + "  c.name AS name, "
                      + "  array_append(p.path, c.parentID) AS path "
                      + "FROM "
                      + "  tree_%d c LEFT JOIN tree_%d p ON c.parentID = p.id ",
                  loop, loop));

      // For robustness in Spark, write output in tables
      spark.sql("DROP TABLE IF EXISTS tree_" + (loop + 1));
      df.write().format("parquet").saveAsTable("tree_" + (loop + 1));

      // when we no longer grow the path, we're done
      Dataset<Row> sizeDF = spark.sql("SELECT max(size(path)) AS maxPath FROM tree_" + (loop + 1));
      maxSize = sizeDF.first().getInt(0);

      loop++;
    }

    // flatten array and write CSV
    Dataset<Row> csv =
        spark.sql(
            "SELECT id, parentId, rank, name, concat_ws('|', path) AS path FROM tree_" + loop);
    csv.repartition(10);
    csv.write().csv(target);
  }
}
