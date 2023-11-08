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

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Takes a gzipped input CSV which has the id and parentID as the first 2 columns and generates all
 * the paths for it written into the output file. Optionally, a prefix can be added to filter the
 * input IDs.
 *
 * <p>Examples for the simple use, using the OToL input and for OToL from checklistbank with a
 * prefix:
 *
 * <pre>
 *   java -Xmx4G tree_sample.csv.gz paths.csv
 *   java -Xmx4G tree_otol.csv.gz paths.csv
 *   java -Xmx4G tree.csv.gz paths.csv 201891
 * </pre>
 */
public class MaterializeInMemory {

  public static void main(String[] args) throws Exception {
    // optionally filter input by some prefix (e.g. 201891 for OToL)
    String prefix = args.length == 3 ? args[2] : null;
    Path input = Paths.get(args[0]);
    String output = args[1];

    Map<String, Node> tree = new HashMap<>(2500000);
    List<Node> roots = new ArrayList<>();
    buildTree(input, prefix, tree, roots);

    try (BufferedWriter out = new BufferedWriter(new FileWriter(output)); ) {
      for (Node root : roots) {
        pathsFor(root, out);
      }
    }
  }

  private static void buildTree(Path file, String prefix, Map<String, Node> tree, List<Node> roots)
      throws IOException {
    AtomicInteger count = new AtomicInteger();

    try (GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file.toFile()));
        BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
        Stream<String> stream = br.lines()) {

      stream.forEach(
          line -> {
            int c = count.addAndGet(1);
            if (c % 1000000 == 0) System.out.println("read " + c);

            String[] fields = line.split(",");
            String id = fields[0];

            // optionally filter by prefix
            if (prefix == null || id.startsWith(prefix)) {

              String parentId = fields[1].length() == 0 ? null : fields[1];

              // out node may have been created as a stub previously
              Node n = tree.containsKey(id) ? tree.get(id) : new Node(id);
              if (!tree.containsKey(id)) tree.put(id, n);

              if (parentId != null) {
                Node parent = tree.get(parentId);
                if (parent == null) {
                  // create the stub
                  parent = new Node(parentId);
                  tree.put(parentId, parent);
                }
                parent.addChild(n);
              } else {
                roots.add(n);
              }
            }
          });

      System.out.println("Tree size: " + tree.size());
      System.out.println("Number of roots: " + roots.size());
    }
  }

  static class Node {
    String id;
    List<Node> children = new ArrayList<>();

    Node(String id) {
      this.id = id;
    }

    void addChild(Node id) {
      children.add(id);
    }
  }

  static void pathsFor(Node root, Writer out) throws IOException {
    if (root == null) return;
    Stack<String> stack = new Stack<>();
    pathsFor(root, stack, out);
  }

  private static void pathsFor(Node node, Stack<String> stack, Writer out) throws IOException {
    if (node == null) return;
    stack.add(node.id);

    out.write(String.join("|", stack));
    out.write("\n");

    if (node.children.isEmpty()) {
      stack.pop();
      return;
    }

    for (Node child : node.children) {
      pathsFor(child, stack, out);
      if (!stack.isEmpty()) stack.pop();
    }
  }
}
