/*
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
package org.apache.accumulo.testing.recovery;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.recovery.HadoopLogCloser;
import org.apache.accumulo.server.master.recovery.LogCloser;
import org.apache.accumulo.testing.continuous.BulkIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * To run the WALTester, copy accumulo-testing-shaded.jar to the Accumulo classpath, then run
 * accumulo org.apache.accumulo.testing.recovery.WALTester \
 *   org.apache.accumulo.server.master.recovery.HadoopLogCloser \
 *   hdfs://localhost:8020/accumulo/file
 */
public class WALTester {
  public static final Logger log = LoggerFactory.getLogger(BulkIngest.class);
  private static final Text HELLO = new Text("hello");

  private AccumuloConfiguration siteConfig;
  private Configuration hadoopConfig;

  private LogCloser logCloser;
  private VolumeManager fs;

  public WALTester(String logCloserClass) throws IOException {
    this.siteConfig = new SiteConfiguration();
    this.hadoopConfig = new Configuration();

    this.logCloser = ConfigurationTypeHelper.getClassInstance((String)null, logCloserClass,
        LogCloser.class, new HadoopLogCloser());

    this.fs = VolumeManagerImpl.get(siteConfig, hadoopConfig);
  }

  public boolean verifyWalOps(Path filePath, boolean syncable, boolean useHsync) throws IOException {
    
    boolean succeeded=true;
    
    FSDataOutputStream out;
    if (syncable) {
      log.info("Creating syncable file");
      out = fs.createSyncable(filePath,  0, (short) 3, 67108864);
    } else {
      log.info("Creating file");
      out = fs.create(filePath, true, 0, (short) 3, 67108864);
    }
    log.info("Writing to file");
    HELLO.write(out);
    if (useHsync) {
      out.hsync();
    } else {
      out.hflush();
    }
    HELLO.write(out);
    log.info("Calling log closer");
    logCloser.close(siteConfig, hadoopConfig, fs, filePath);

    boolean gotException = false;
    try {
      log.info("Writing to file after log close");
      HELLO.write(out);
      log.info("Syncing after log close");
      if (useHsync) {
        out.hsync();
      } else {
        out.hflush();
      }
    } catch (Exception e) {
      log.info("Got exception on write+sync after close as expected", e);
      gotException = true;
    }
    if (!gotException) {
      log.error("No exception on write+sync after log was closed");
      succeeded = false;
    }

    try {
      if (out != null) {
        out.close();
        succeeded = false;
      }
    } catch (Exception e) {
      log.info("Got exception on close as expected", e);
    }

    log.info("Reading file");
    Text t = new Text();
    try (FSDataInputStream in = fs.open(filePath)) {
      int count = 0;
      while (in.available() > 0) {
        t.readFields(in);
        log.info("Read text " + t);
        count++;
      }
      if (count != 1) {
        log.error("Expected to read 1 flushed entry from file, but got {}", count);
        succeeded = false;
      }
    }
    
    return succeeded;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("Expected <logCloserClass> <basePath> arguments.");
    }

    WALTester walTester = new WALTester(args[0]);
    Path basePath = new Path(args[1]);

    boolean succeeded = true;
    
    succeeded &= walTester.verifyWalOps(new Path(basePath, "1"), false, true);
    succeeded &= walTester.verifyWalOps(new Path(basePath, "2"), false, false);
    succeeded &= walTester.verifyWalOps(new Path(basePath, "3"), true, true);
    succeeded &= walTester.verifyWalOps(new Path(basePath, "4"), true, false);
    
    if(!succeeded) System.exit(1);
  }
}
