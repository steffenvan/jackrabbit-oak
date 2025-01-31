/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.index;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

/**
 * Utility class to safely share {@link DirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 * 
 * @see SearcherManager
 * 
 * @lucene.experimental
 */
public final class ReaderManager extends ReferenceManager<DirectoryReader> {

  /**
   * Creates and returns a new ReaderManager from the given
   * {@link IndexWriter}.
   * 
   * @param writer
   *          the IndexWriter to open the IndexReader from.
   * @param applyAllDeletes
   *          If <code>true</code>, all buffered deletes will be applied (made
   *          visible) in the {@link IndexSearcher} / {@link DirectoryReader}.
   *          If <code>false</code>, the deletes may or may not be applied, but
   *          remain buffered (in IndexWriter) so that they will be applied in
   *          the future. Applying deletes can be costly, so if your app can
   *          tolerate deleted documents being returned you might gain some
   *          performance by passing <code>false</code>. See
   *          {@link DirectoryReader#openIfChanged(DirectoryReader, IndexWriter, boolean)}.
   * 
   * @throws IOException If there is a low-level I/O error
   */
  public ReaderManager(IndexWriter writer, boolean applyAllDeletes) throws IOException {
    current = DirectoryReader.open(writer, applyAllDeletes);
  }
  
  /**
   * Creates and returns a new ReaderManager from the given {@link Directory}. 
   * @param dir the directory to open the DirectoryReader on.
   *        
   * @throws IOException If there is a low-level I/O error
   */
  public ReaderManager(Directory dir) throws IOException {
    current = DirectoryReader.open(dir);
  }

  @Override
  protected void decRef(DirectoryReader reference) throws IOException {
    reference.decRef();
  }
  
  @Override
  protected DirectoryReader refreshIfNeeded(DirectoryReader referenceToRefresh) throws IOException {
    return DirectoryReader.openIfChanged(referenceToRefresh);
  }
  
  @Override
  protected boolean tryIncRef(DirectoryReader reference) {
    return reference.tryIncRef();
  }

  @Override
  protected int getRefCount(DirectoryReader reference) {
    return reference.getRefCount();
  }

}
