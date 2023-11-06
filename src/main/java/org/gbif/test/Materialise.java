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

@Builder(toBuilder = true)
public class Materialise implements Serializable {
  private final String source;
  private final String target;

  public static void main(String[] args) {
    Materialise.builder().source("/tmp/tree.csv").target("/tmp/tree-materialised").build().run();
  }

  public void run() {
    SparkSession spark =
        SparkSession.builder().appName("Materialise path").enableHiveSupport().getOrCreate();

    spark.sql("use tim");
    spark.sparkContext().conf().set("hive.exec.compress.output", "true");

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
            .withColumn("lineage", functions.lit(null).cast("string"));

    spark.sql("DROP TABLE IF EXISTS tree_0");
    input.write().format("parquet").saveAsTable("tree_0");

    // Loop doing a self join and copying down the parent lineage each time rewriting the full table
    // into a new table. With sufficient looping, the lineage propagates to the leaf nodes
    Dataset<Row> df = null;
    for (int i = 0; i < 30; i++) {
      df =
          spark.sql(
              String.format(
                  "SELECT "
                      + "  c.id AS id, "
                      + "  c.parentID AS parentID, "
                      + "  c.rank AS rank, "
                      + "  c.name AS name, "
                      + "  concat_ws('|', p.lineage, c.parentID) AS lineage "
                      + "FROM "
                      + "  tree_%d c LEFT JOIN tree_%d p ON c.parentID = p.id ",
                  i, i));

      spark.sql("DROP TABLE IF EXISTS tree_" + (i + 1));
      df.write().format("parquet").saveAsTable("tree_" + (i + 1));
    }
    df.repartition(10);
    df.write().csv(target);
  }
}
