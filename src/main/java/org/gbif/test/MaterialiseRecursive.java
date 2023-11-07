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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import lombok.Builder;

@Builder(toBuilder = true)
public class MaterialiseRecursive implements Serializable {
  private final String source;
  private final String target;

  public static void main(String[] args) {
    MaterialiseRecursive.builder().source("/tmp/tree.csv").target("/tmp/tree_output").build().run();
  }

  public void run() {
    SparkSession spark =
        SparkSession.builder().appName("Materialise path").enableHiveSupport().getOrCreate();

    spark.sql("use tim");
    spark.sparkContext().conf().set("hive.exec.compress.output", "true");

    // read input adding empty lineage and depth columns
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
            .csv(source);
    spark.sql("DROP TABLE IF EXISTS tree_input");
    input.write().format("parquet").saveAsTable("tree_input");

    Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, parentID, rank, name, '' AS path "
                    + "FROM tree_input "
                    + "WHERE parentID IS NULL"));
    df.createOrReplaceTempView("tree");

    Dataset<Row> tree = df;
    while (df.count() > 0) {
      df =
          spark.sql(
              "SELECT"
                  + "  c.id AS id,"
                  + "  c.parentID AS parentID,"
                  + "  c.rank AS rank,"
                  + "  c.name AS name,"
                  + "  concat_ws('|', p.path, c.id) AS path" // TODO: use arrays
                  + " FROM"
                  + "  tree p JOIN tree_input c ON c.parentID = p.id"
                  + "    LEFT JOIN tree t ON t.id = c.id"
                  + " WHERE"
                  + "  t.id IS NULL");

      tree = tree.union(df);
      tree.createOrReplaceTempView("tree");
    }

    spark.sql("DROP TABLE IF EXISTS tree_output");
    tree.write().format("parquet").saveAsTable("tree_output");
    tree.repartition(10);
    tree.write().csv(target);
  }
}
