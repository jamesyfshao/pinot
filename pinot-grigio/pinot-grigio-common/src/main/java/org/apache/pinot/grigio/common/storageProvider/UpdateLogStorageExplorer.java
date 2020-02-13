/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.grigio.common.storageProvider;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.IOException;
import java.util.Collection;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * command line tools for debug pinot server by allowing us to interatively explore the update log data in pinot server/kc
 * usage:
 * $java -cp <pinot-jar-path> org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageExplorer <path to virtual column base path>
 * you can then input the table name (with _REALTIME postfix) and segment to load data
 * after this, you can enter the offset you want to explore the update log data at
 */
public class UpdateLogStorageExplorer {
  public static void main(String[] args) throws IOException {
    Preconditions.checkState(args.length > 0, "need basepath as first parameter");
    String basePath = args[0];

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(UpdateLogStorageProvider.BASE_PATH_CONF_KEY, basePath);

    UpdateLogStorageProvider.init(conf);
    UpdateLogStorageProvider provider = UpdateLogStorageProvider.getInstance();

    Scanner reader = new Scanner(System.in);
    System.out.println("please input the tablename and segment name to load");
    String input = reader.nextLine();
    String[] inputSplits = input.split(" ");
    Preconditions.checkState(inputSplits.length == 2, "expect input data to be 'tableName segmentName'");
    String tableName = inputSplits[0];
    if (!tableName.endsWith("_REALTIME")) {
      tableName = tableName + "_REALTIME";
    }
    String segmentName = inputSplits[1];

    provider.loadTable(tableName);
    UpdateLogEntrySet updateLogEntrySet = provider.getAllMessages(tableName, segmentName);
    Multimap<Long, UpdateLogAndPos> map = ArrayListMultimap.create();
    System.out.println("update log size: " + updateLogEntrySet.size());
    AtomicInteger pos = new AtomicInteger(0);
    updateLogEntrySet.forEach(u -> {
      map.put(u.getOffset(), new UpdateLogAndPos(u, pos.getAndIncrement()));
    });

    while (true) {
      System.out.println("input the offset");
      long offset = reader.nextLong();
      Collection<UpdateLogAndPos> result = map.get(offset);
      System.out.println("associated update logs size: " + result.size());
      for (UpdateLogAndPos entry: result) {
        System.out.println("content: " + entry.logEntry.toString() + " pos " + entry.pos);
      }
    }
  }

  static class UpdateLogAndPos {
    public UpdateLogEntry logEntry;
    public int pos;

    public UpdateLogAndPos(UpdateLogEntry entry, int pos) {
      this.logEntry = entry;
      this.pos = pos;
    }
  }
}
