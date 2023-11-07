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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class AppendArray implements UDF2<Seq<String>, String, Seq<String>> {
  @Override
  public Seq<String> call(Seq<String> array, String s) {
    List<String> out = new ArrayList<>(30);
    if (array != null && array.size() > 0)
      out.addAll(JavaConverters.seqAsJavaListConverter(array).asJava());
    if (s != null) out.add(s);
    return JavaConverters.asScalaBufferConverter(out).asScala();
  }

  public static void register(String name, SparkSession spark) {
    spark.udf().register(name, new AppendArray(), DataTypes.createArrayType(DataTypes.StringType));
  }
}
