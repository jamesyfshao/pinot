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
package org.apache.pinot.core.indexsegment.mutable;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.core.indexsegment.IndexSegmentUtils;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.readerwriter.BaseSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import org.apache.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.io.readerwriter.impl.VarByteSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.core.realtime.impl.dictionary.BaseMutableDictionary;
import org.apache.pinot.core.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.core.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeInvertedIndexReader;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeLuceneTextIndexReader;
import org.apache.pinot.core.realtime.impl.nullvalue.RealtimeNullValueVectorReaderWriter;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.data.source.ColumnDataSource;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.core.startree.v2.StarTreeV2;
import org.apache.pinot.core.util.FixedIntArray;
import org.apache.pinot.core.util.FixedIntArrayOffHeapIdMap;
import org.apache.pinot.core.util.IdMap;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RowMetadata;
import org.roaringbitmap.IntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MutableSegmentImpl implements MutableSegment {
  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final String RECORD_ID_MAP = "__recordIdMap__";
  private static final int EXPECTED_COMPRESSION = 1000;
  private static final int MIN_ROWS_TO_INDEX = 1000_000; // Min size of recordIdMap for updatable metrics.
  private static final int MIN_RECORD_ID_MAP_CACHE_SIZE = 10000; // Min overflow map size for updatable metrics.

  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT = 100;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT = 100_000;

  protected final Logger _logger;
  private final long _startTimeMillis = System.currentTimeMillis();

  protected final String _tableName;
  protected final String _segmentName;
  protected final Schema _schema;
  private final int _capacity;
  private final SegmentMetadata _segmentMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final SegmentPartitionConfig _segmentPartitionConfig;
  private final boolean _nullHandlingEnabled;

  private final Map<String, BaseMutableDictionary> _dictionaryMap = new HashMap<>();
  private final Map<String, DataFileReader> _indexReaderWriterMap = new HashMap<>();
  private final Map<String, Integer> _maxNumValuesMap = new HashMap<>();
  private final Map<String, InvertedIndexReader> _invertedIndexMap = new HashMap<>();
  private final Map<String, BloomFilterReader> _bloomFilterMap = new HashMap<>();
  private final Map<String, RealtimeNullValueVectorReaderWriter> _nullValueVectorMap = new HashMap<>();
  private final IdMap<FixedIntArray> _recordIdMap;
  private boolean _aggregateMetrics;

  protected volatile int _numDocsIndexed = 0;

  // to compute the rolling interval
  protected volatile long _minTime = Long.MAX_VALUE;
  protected volatile long _maxTime = Long.MIN_VALUE;
  protected final int _numKeyColumns;

  // Cache the physical (non-virtual) field specs
  private final Collection<FieldSpec> _physicalFieldSpecs;
  private final Collection<DimensionFieldSpec> _physicalDimensionFieldSpecs;
  private final Collection<MetricFieldSpec> _physicalMetricFieldSpecs;

  // Cache some virtual columns
  protected final Map<String, VirtualColumnProvider> _virtualColumnProviderMap = new HashMap<>();
  protected final Map<String, Dictionary> _virtualColumnDictionary = new HashMap<>();
  protected final Map<String, DataFileReader> _virtualColumnIndexReader = new HashMap<>();

  // default message metadata
  private volatile long _lastIndexedTimeMs = Long.MIN_VALUE;
  private volatile long _latestIngestionTimeMs = Long.MIN_VALUE;

  private RealtimeLuceneReaders _realtimeLuceneReaders;

  public MutableSegmentImpl(RealtimeSegmentConfig config) {
    _tableName = config.getTableName();
    _segmentName = config.getSegmentName();
    _schema = config.getSchema();
    _capacity = config.getCapacity();
    _segmentMetadata = new SegmentMetadataImpl(config.getRealtimeSegmentZKMetadata(), _schema) {
      @Override
      public int getTotalDocs() {
        return _numDocsIndexed;
      }

      @Override
      public int getTotalRawDocs() {
        // In realtime total docs and total raw docs are the same currently.
        return _numDocsIndexed;
      }

      @Override
      public long getLastIndexedTimestamp() {
        return _lastIndexedTimeMs;
      }

      @Override
      public long getLatestIngestionTimestamp() {
        return _latestIngestionTimeMs;
      }
    };

    _offHeap = config.isOffHeap();
    _memoryManager = config.getMemoryManager();
    _statsHistory = config.getStatsHistory();
    _segmentPartitionConfig = config.getSegmentPartitionConfig();
    _nullHandlingEnabled = config.isNullHandlingEnabled();

    Collection<FieldSpec> allFieldSpecs = _schema.getAllFieldSpecs();
    List<FieldSpec> physicalFieldSpecs = new ArrayList<>(allFieldSpecs.size());
    List<FieldSpec> virtualFieldSpecs = new ArrayList<>(allFieldSpecs.size());
    List<DimensionFieldSpec> physicalDimensionFieldSpecs = new ArrayList<>(_schema.getDimensionNames().size());
    List<MetricFieldSpec> physicalMetricFieldSpecs = new ArrayList<>(_schema.getMetricNames().size());

    for (FieldSpec fieldSpec : allFieldSpecs) {
      if (!fieldSpec.isVirtualColumn()) {
        physicalFieldSpecs.add(fieldSpec);

        FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
        if (fieldType == FieldSpec.FieldType.DIMENSION) {
          physicalDimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        } else if (fieldType == FieldSpec.FieldType.METRIC) {
          physicalMetricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        }
      } else {
        virtualFieldSpecs.add(fieldSpec);
      }
    }
    _physicalFieldSpecs = Collections.unmodifiableCollection(physicalFieldSpecs);
    _physicalDimensionFieldSpecs = Collections.unmodifiableCollection(physicalDimensionFieldSpecs);
    _physicalMetricFieldSpecs = Collections.unmodifiableCollection(physicalMetricFieldSpecs);
    _numKeyColumns = _physicalDimensionFieldSpecs.size() + 1;  // Add 1 for time column

    _logger =
        LoggerFactory.getLogger(MutableSegmentImpl.class.getName() + "_" + _segmentName + "_" + config.getStreamName());

    Set<String> noDictionaryColumns = config.getNoDictionaryColumns();

    Set<String> invertedIndexColumns = config.getInvertedIndexColumns();
    Set<String> textIndexColumns = config.getTextIndexColumns();

    int avgNumMultiValues = config.getAvgNumMultiValues();


    // Initialize for each column
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      _maxNumValuesMap.put(column, 0);

      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index
      FieldSpec.DataType dataType = fieldSpec.getDataType();
      boolean isFixedWidthColumn = dataType.isFixedWidth();
      int forwardIndexColumnSize = -1;
      if (isNoDictionaryColumn(noDictionaryColumns, invertedIndexColumns, textIndexColumns, fieldSpec, column)) {
        // no dictionary
        // each forward index entry will be equal to size of data for that row
        // For INT, LONG, FLOAT, DOUBLE it is equal to the number of fixed bytes used to store the value,
        if (isFixedWidthColumn) {
          forwardIndexColumnSize = dataType.size();
        }
      } else {
        // dictionary encoded index
        // each forward index entry will contain a 4 byte dictionary ID
        forwardIndexColumnSize = FieldSpec.DataType.INT.size();
        int dictionaryColumnSize;
        if (dataType == FieldSpec.DataType.STRING) {
          dictionaryColumnSize = _statsHistory.getEstimatedAvgColSize(column);
        } else {
          dictionaryColumnSize = dataType.size();
        }
        // NOTE: preserve 10% buffer for cardinality to reduce the chance of re-sizing the dictionary
        int estimatedCardinality = (int) (_statsHistory.getEstimatedCardinality(column) * 1.1);
        String allocationContext = buildAllocationContext(_segmentName, column, V1Constants.Dict.FILE_EXTENSION);
        BaseMutableDictionary dictionary = MutableDictionaryFactory
            .getMutableDictionary(dataType, _offHeap, _memoryManager, dictionaryColumnSize,
                Math.min(estimatedCardinality, _capacity), allocationContext);
        _dictionaryMap.put(column, dictionary);

        // Even though the column is defined as 'no-dictionary' in the config, we did create dictionary for consuming segment.
        noDictionaryColumns.remove(column);
      }

      DataFileReader indexReaderWriter;

      // create forward index reader/writer
      if (forwardIndexColumnSize == -1) {
        // for STRING/BYTES SV column, we support raw index in consuming segments
        // RealtimeSegmentStatsHistory does not have the stats for no-dictionary columns
        // from previous consuming segments
        // TODO: Add support for updating RealtimeSegmentStatsHistory with average column value size for no dictionary columns as well
        // TODO: Use the stats to get estimated average length
        // Use a smaller capacity as opposed to segment flush size
        int initialCapacity = Math.min(_capacity, NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
        String allocationContext = buildAllocationContext(_segmentName, column, V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        indexReaderWriter = new VarByteSingleColumnSingleValueReaderWriter(_memoryManager, allocationContext, initialCapacity, NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT);
      } else {
        // two possible cases can lead here:
        // (1) dictionary encoded forward index
        // (2) raw forward index for fixed width types -- INT, LONG, FLOAT, DOUBLE
        if (fieldSpec.isSingleValueField()) {
          // SV column -- both dictionary encoded and raw index are supported on SV
          // columns for both fixed and variable width types
          String allocationContext =
              buildAllocationContext(_segmentName, column, V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
          indexReaderWriter = new FixedByteSingleColumnSingleValueReaderWriter(_capacity, forwardIndexColumnSize, _memoryManager,
              allocationContext);
        } else {
          // MV column -- only dictionary encoded index is supported on MV columns
          // for both fixed and variable width types
          // TODO: Start with a smaller capacity on FixedByteSingleColumnMultiValueReaderWriter and let it expand
          String allocationContext =
              buildAllocationContext(_segmentName, column, V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
          indexReaderWriter =
              new FixedByteSingleColumnMultiValueReaderWriter(MAX_MULTI_VALUES_PER_ROW, avgNumMultiValues, _capacity,
                  forwardIndexColumnSize, _memoryManager, allocationContext);
        }
      }

      _indexReaderWriterMap.put(column, indexReaderWriter);


      if (invertedIndexColumns.contains(column)) {
        _invertedIndexMap.put(column, new RealtimeInvertedIndexReader());
      }

      if (_nullHandlingEnabled) {
        _nullValueVectorMap.put(column, new RealtimeNullValueVectorReaderWriter());
      }

      if (textIndexColumns.contains(column)) {
        RealtimeLuceneTextIndexReader realtimeLuceneIndexReader = new RealtimeLuceneTextIndexReader(column, new File(config.getConsumerDir()), _segmentName);
        _invertedIndexMap.put(column, realtimeLuceneIndexReader);
        if (_realtimeLuceneReaders == null) {
          _realtimeLuceneReaders= new RealtimeLuceneReaders(_segmentName);
        }
        _realtimeLuceneReaders.addReader(realtimeLuceneIndexReader);
      }
    }

    for (FieldSpec fieldSpec : virtualFieldSpecs) {
      String column = fieldSpec.getName();
      VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, _capacity, true);
      final VirtualColumnProvider provider =
              VirtualColumnProviderFactory.buildProvider(fieldSpec.getVirtualColumnProvider());
      if (provider == null) {
        throw new RuntimeException(String.format("failed to create virtual segment provider for field %s", fieldSpec.getName()));
      }
      DataFileReader reader = provider.buildReader(virtualColumnContext);
      Dictionary dictionary = provider.buildDictionary(virtualColumnContext);
      _virtualColumnProviderMap.put(column, provider);
      _virtualColumnIndexReader.put(column, reader);
      _virtualColumnDictionary.put(column, dictionary);
    }

    if (_realtimeLuceneReaders != null) {
      // add the realtime lucene index readers to the global queue for refresh task to pick up
      RealtimeLuceneIndexRefreshState realtimeLuceneIndexRefreshState = RealtimeLuceneIndexRefreshState.getInstance();
      realtimeLuceneIndexRefreshState.addRealtimeReadersToQueue(_realtimeLuceneReaders);
    }

    // Metric aggregation can be enabled only if config is specified, and all dimensions have dictionary,
    // and no metrics have dictionary. If not enabled, the map returned is null.
    _recordIdMap = enableMetricsAggregationIfPossible(config, noDictionaryColumns);
  }

  /**
   * Decide whether a given column should be dictionary encoded or not
   * @param noDictionaryColumns no dictionary column set
   * @param invertedIndexColumns inverted index column set
   * @param fieldSpec field spec of column
   * @param column column name
   * @return true if column is no-dictionary, false if dictionary encoded
   */
  private boolean isNoDictionaryColumn(Set<String> noDictionaryColumns, Set<String> invertedIndexColumns,
      Set<String> textIndexColumns, FieldSpec fieldSpec, String column) {
    return textIndexColumns.contains(column) || (noDictionaryColumns.contains(column) && fieldSpec.isSingleValueField()
        && !invertedIndexColumns.contains(column));
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    return _segmentPartitionConfig;
  }

  public long getMinTime() {
    return _minTime;
  }

  public long getMaxTime() {
    return _maxTime;
  }

  @Override
  public boolean index(GenericRow row, @Nullable RowMetadata rowMetadata) {
    boolean canTakeMore;
    // Update dictionary first
    Map<String, Object> dictIdMap = updateDictionary(row);

    int numDocs = _numDocsIndexed;

    // If metrics aggregation is enabled and if the dimension values were already seen, this will return existing docId,
    // else this will return a new docId.
    int docId = getOrCreateDocId(dictIdMap);

    // docId == numDocs implies new docId.
    if (docId == numDocs) {
      // Add forward and inverted indices for new document.
      addForwardIndex(row, docId, dictIdMap);
      addInvertedIndex(row, docId, dictIdMap);
      if (_nullHandlingEnabled) {
        handleNullValues(row, docId);
      }

      // Update number of document indexed at last to make the latest record queryable
      canTakeMore = _numDocsIndexed++ < _capacity;
      postProcessRecords(row, docId);
    } else {
      Preconditions
          .checkState(_aggregateMetrics, "Invalid document-id during indexing: " + docId + " expected: " + numDocs);
      // Update metrics for existing document.
      canTakeMore = aggregateMetrics(row, docId);
    }

    _lastIndexedTimeMs = System.currentTimeMillis();

    if (rowMetadata != null && rowMetadata.getIngestionTimeMs() != Long.MIN_VALUE) {
      _latestIngestionTimeMs = Math.max(_latestIngestionTimeMs, rowMetadata.getIngestionTimeMs());
    }
    return canTakeMore;
  }

  private Map<String, Object> updateDictionary(GenericRow row) {
    Map<String, Object> dictIdMap = new HashMap<>();
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);

      BaseMutableDictionary dictionary = _dictionaryMap.get(column);
      if (dictionary != null) {
        if (fieldSpec.isSingleValueField()) {
          dictIdMap.put(column, dictionary.index(value));
        } else {
          int[] dictIds = dictionary.index((Object[]) value);
          dictIdMap.put(column, dictIds);
          int numValues = dictIds.length;
          // Update max number of values for multi-value column
          if (_maxNumValuesMap.get(column) < numValues) {
            _maxNumValuesMap.put(column, numValues);
          }

          // No need to update min/max time value as time column cannot be multi-valued
          continue;
        }
      }

      // Update min/max value for time column
      if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.TIME)) {
        long timeValue;
        if (value instanceof Number) {
          timeValue = ((Number) value).longValue();
        } else {
          timeValue = Long.valueOf(value.toString());
        }
        _minTime = Math.min(_minTime, timeValue);
        _maxTime = Math.max(_maxTime, timeValue);
      }
    }
    return dictIdMap;
  }

  private void addForwardIndex(GenericRow row, int docId, Map<String, Object> dictIdMap) {
    // Store dictionary Id(s) for columns with dictionary
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);
      if (fieldSpec.isSingleValueField()) {
        // SV column
        BaseSingleColumnSingleValueReaderWriter indexReaderWriter =
            (BaseSingleColumnSingleValueReaderWriter) _indexReaderWriterMap.get(column);
        Integer dictId = (Integer) dictIdMap.get(column);
        if (dictId != null) {
          // SV Column with dictionary
          indexReaderWriter.setInt(docId, dictId);
        } else {
          // No-dictionary SV column
          FieldSpec.DataType dataType = fieldSpec.getDataType();
          switch (dataType) {
            case INT:
              indexReaderWriter.setInt(docId, (Integer) value);
              break;
            case LONG:
              indexReaderWriter.setLong(docId, (Long) value);
              break;
            case FLOAT:
              indexReaderWriter.setFloat(docId, (Float) value);
              break;
            case DOUBLE:
              indexReaderWriter.setDouble(docId, (Double) value);
              break;
            case STRING:
              indexReaderWriter.setString(docId, (String) value);
              break;
            case BYTES:
              indexReaderWriter.setBytes(docId, (byte[]) value);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
          }
        }
      } else {
        // MV column: always dictionary encoded
        int[] dictIds = (int[]) dictIdMap.get(column);
        ((FixedByteSingleColumnMultiValueReaderWriter) _indexReaderWriterMap.get(column)).setIntArray(docId, dictIds);
      }
    }
  }

  private void addInvertedIndex(GenericRow row, int docId, Map<String, Object> dictIdMap) {
    // Update inverted index at last
    // NOTE: inverted index have to be updated at last because once it gets updated, the latest record will become
    // queryable
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      InvertedIndexReader invertedIndex = _invertedIndexMap.get(column);
      if (invertedIndex != null) {
        if (invertedIndex instanceof RealtimeLuceneTextIndexReader) {
          ((RealtimeLuceneTextIndexReader)invertedIndex).addDoc(row.getValue(column), docId);
        } else {
          RealtimeInvertedIndexReader realtimeInvertedIndexReader = (RealtimeInvertedIndexReader)invertedIndex;
          if (fieldSpec.isSingleValueField()) {
            realtimeInvertedIndexReader.add(((Integer) dictIdMap.get(column)), docId);
          } else {
            int[] dictIds = (int[]) dictIdMap.get(column);
            for (int dictId : dictIds) {
              realtimeInvertedIndexReader.add(dictId, docId);
            }
          }
        }

      }
    }
  }

  /**
   * Check if the row has any null fields and update the
   * column null value vectors accordingly
   * @param row specifies row being ingested
   * @param docId specified docId for this row
   */
  private void handleNullValues(GenericRow row, int docId) {
    if (!row.hasNullValues()) {
      return;
    }

    for (String columnName : row.getNullValueFields()) {
      _nullValueVectorMap.get(columnName).setNull(docId);
    }
  }

  protected abstract void postProcessRecords(GenericRow row, int docId);

  private boolean aggregateMetrics(GenericRow row, int docId) {
    for (MetricFieldSpec metricFieldSpec : _physicalMetricFieldSpecs) {
      String column = metricFieldSpec.getName();
      Object value = row.getValue(column);
      FixedByteSingleColumnSingleValueReaderWriter indexReaderWriter =
          (FixedByteSingleColumnSingleValueReaderWriter) _indexReaderWriterMap.get(column);

      FieldSpec.DataType dataType = metricFieldSpec.getDataType();
      switch (dataType) {
        case INT:
          indexReaderWriter.setInt(docId, (Integer) value + indexReaderWriter.getInt(docId));
          break;
        case LONG:
          indexReaderWriter.setLong(docId, (Long) value + indexReaderWriter.getLong(docId));
          break;
        case FLOAT:
          indexReaderWriter.setFloat(docId, (Float) value + indexReaderWriter.getFloat(docId));
          break;
        case DOUBLE:
          indexReaderWriter.setDouble(docId, (Double) value + indexReaderWriter.getDouble(docId));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
      }
    }
    return true;
  }

  @Override
  public int getNumDocsIndexed() {
    return _numDocsIndexed;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public Set<String> getColumnNames() {
    // Return all column names, virtual and physical.
    return _schema.getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    HashSet<String> physicalColumnNames = new HashSet<>();

    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      physicalColumnNames.add(fieldSpec.getName());
    }

    return physicalColumnNames;
  }

  @Override
  public ColumnDataSource getDataSource(String columnName) {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
    if (fieldSpec.isVirtualColumn()) {
      // FIXME
      VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, _numDocsIndexed, true);
      VirtualColumnProvider virtualColumnProvider = _virtualColumnProviderMap.get(columnName);
      return new ColumnDataSource(virtualColumnProvider.buildColumnIndexContainer(virtualColumnContext),
          virtualColumnProvider.buildMetadata(virtualColumnContext));
    } else {
      return new ColumnDataSource(fieldSpec, _numDocsIndexed, _maxNumValuesMap.get(columnName),
          _indexReaderWriterMap.get(columnName), _invertedIndexMap.get(columnName), _dictionaryMap.get(columnName),
          _bloomFilterMap.get(columnName), _nullValueVectorMap.get(columnName));
    }
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  /**
   * Returns a record that contains only physical columns
   * @param docId document ID
   * @param reuse a GenericRow object that will be re-used if provided. Otherwise, this method will allocate a new one
   * @return Generic row with physical columns of the specified row.
   */
  public GenericRow getRecord(int docId, GenericRow reuse) {
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      Object value = IndexSegmentUtils
          .getValue(docId, fieldSpec, _indexReaderWriterMap.get(column), _dictionaryMap.get(column),
              _maxNumValuesMap.getOrDefault(column, 0));
      reuse.putValue(column, value);

      if (_nullHandlingEnabled) {
        NullValueVectorReader reader = _nullValueVectorMap.get(column);
        // If column has null value for this docId, set that accordingly in GenericRow
        if (reader.isNull(docId)) {
          reuse.putDefaultNullValue(column, value);
        }
      }
    }
    return reuse;
  }

  @Override
  public void destroy() {
    _logger.info("Trying to close RealtimeSegmentImpl : {}", _segmentName);

    // Gather statistics for off-heap mode
    if (_offHeap) {
      if (_numDocsIndexed > 0) {
        int numSeconds = (int) ((System.currentTimeMillis() - _startTimeMillis) / 1000);
        long totalMemBytes = _memoryManager.getTotalAllocatedBytes();
        _logger
            .info("Segment used {} bytes of memory for {} rows consumed in {} seconds", totalMemBytes, _numDocsIndexed,
                numSeconds);

        RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
        for (Map.Entry<String, BaseMutableDictionary> entry : _dictionaryMap.entrySet()) {
          String columnName = entry.getKey();
          BaseOffHeapMutableDictionary dictionary = (BaseOffHeapMutableDictionary) entry.getValue();
          RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
          columnStats.setCardinality(dictionary.length());
          columnStats.setAvgColumnSize(dictionary.getAvgValueSize());
          segmentStats.setColumnStats(columnName, columnStats);
        }
        segmentStats.setNumRowsConsumed(_numDocsIndexed);
        segmentStats.setNumRowsIndexed(_numDocsIndexed);
        segmentStats.setMemUsedBytes(totalMemBytes);
        segmentStats.setNumSeconds(numSeconds);
        _statsHistory.addSegmentStats(segmentStats);
      }
    }

    for (DataFileReader dfReader : _indexReaderWriterMap.values()) {
      try {
        dfReader.close();
      } catch (IOException e) {
        _logger.error("Failed to close index. Service will continue with potential memory leak, error: ", e);
        // fall through to close other segments
      }
    }
    // clear map now that index is closed to prevent accidental usage
    _indexReaderWriterMap.clear();

    for (InvertedIndexReader index : _invertedIndexMap.values()) {
      if (index instanceof RealtimeInvertedIndexReader) {
        ((RealtimeInvertedIndexReader)index).close();
      }
    }

    if (_realtimeLuceneReaders != null) {
      // set this to true as a way of signalling the refresh task thread to
      // not attempt refresh on this segment here onwards
      _realtimeLuceneReaders.getLock().lock();
      _realtimeLuceneReaders.setSegmentDestroyed();
      try {
        for (RealtimeLuceneTextIndexReader realtimeLuceneReader : _realtimeLuceneReaders.getRealtimeLuceneReaders()) {
          // close each realtime lucene reader for this segment
          realtimeLuceneReader.close();
        }
        // clear the list.
        _realtimeLuceneReaders.clearRealtimeReaderList();
      } finally {
        _realtimeLuceneReaders.getLock().unlock();
      }
    }

    for (Map.Entry<String, BaseMutableDictionary> entry : _dictionaryMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        _logger.error("Could not close dictionary for column {}", entry.getKey());
      }
    }
    _invertedIndexMap.clear();
    _segmentMetadata.close();
    try {
      _memoryManager.close();
    } catch (IOException e) {
      _logger.error("Could not close memory manager", e);
    }

    // Clear the recordId map.
    if (_recordIdMap != null) {
      _recordIdMap.clear();
    }
  }

  /**
   * Returns the docIds to use for iteration when the data is sorted by the given column.
   * <p>Called only by realtime record reader.
   *
   * @param column The column to use for sorting
   * @return The docIds to use for iteration
   */
  public int[] getSortedDocIdIterationOrderWithSortedColumn(String column) {
    BaseMutableDictionary dictionary = _dictionaryMap.get(column);
    int numValues = dictionary.length();

    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      dictIds[i] = i;
    }

    IntArrays.quickSort(dictIds, (dictId1, dictId2) -> dictionary.compare(dictId1, dictId2));
    RealtimeInvertedIndexReader invertedIndex = (RealtimeInvertedIndexReader)_invertedIndexMap.get(column);
    int[] docIds = new int[_numDocsIndexed];
    int docIdIndex = 0;
    for (int dictId : dictIds) {
      IntIterator intIterator = invertedIndex.getDocIds(dictId).getIntIterator();
      while (intIterator.hasNext()) {
        docIds[docIdIndex++] = intIterator.next();
      }
    }

    // Sanity check
    Preconditions.checkState(_numDocsIndexed == docIdIndex,
        "The number of documents indexed: %s is not equal to the number of sorted documents: %s", _numDocsIndexed,
        docIdIndex);

    return docIds;
  }

  /**
   * Helper method that builds allocation context that includes segment name, column name, and index type.
   *
   * @param segmentName Name of segment.
   * @param columnName Name of column.
   * @param indexType Index type.
   * @return Allocation context built from segment name, column name and index type.
   */
  private String buildAllocationContext(String segmentName, String columnName, String indexType) {
    return segmentName + ":" + columnName + indexType;
  }

  private int getOrCreateDocId(Map<String, Object> dictIdMap) {
    if (!_aggregateMetrics) {
      return _numDocsIndexed;
    }

    int i = 0;
    int[] dictIds = new int[_numKeyColumns]; // dimensions + time column.

    // FIXME: this for loop breaks for multi value dimensions. https://github.com/apache/incubator-pinot/issues/3867
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      dictIds[i++] = (Integer) dictIdMap.get(fieldSpec.getName());
    }

    String timeColumnName = _schema.getTimeColumnName();
    if (timeColumnName != null) {
      dictIds[i] = (Integer) dictIdMap.get(timeColumnName);
    }
    return _recordIdMap.put(new FixedIntArray(dictIds));
  }

  /**
   * Helper method to enable/initialize aggregation of metrics, based on following conditions:
   * <ul>
   *   <li> Config to enable aggregation of metrics is specified. </li>
   *   <li> All dimensions and time are dictionary encoded. This is because an integer array containing dictionary id's
   *        is used as key for dimensions to record Id map. </li>
   *   <li> None of the metrics are dictionary encoded. </li>
   *   <li> All columns should be single-valued (see https://github.com/apache/incubator-pinot/issues/3867)</li>
   * </ul>
   *
   * TODO: Eliminate the requirement on dictionary encoding for dimension and metric columns.
   *
   * @param config Segment config.
   * @param noDictionaryColumns Set of no dictionary columns.
   *
   * @return Map from dictionary id array to doc id, null if metrics aggregation cannot be enabled.
   */
  private IdMap<FixedIntArray> enableMetricsAggregationIfPossible(RealtimeSegmentConfig config,
      Set<String> noDictionaryColumns) {
    _aggregateMetrics = config.aggregateMetrics();
    if (!_aggregateMetrics) {
      _logger.info("Metrics aggregation is disabled.");
      return null;
    }

    // All metric columns should have no-dictionary index.
    // All metric columns must be single value
    for (FieldSpec fieldSpec : _physicalMetricFieldSpecs) {
      String metric = fieldSpec.getName();
      if (!noDictionaryColumns.contains(metric)) {
        _logger
            .warn("Metrics aggregation cannot be turned ON in presence of dictionary encoded metrics, eg: {}", metric);
        _aggregateMetrics = false;
        break;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger
            .warn("Metrics aggregation cannot be turned ON in presence of multi-value metric columns, eg: {}", metric);
        _aggregateMetrics = false;
        break;
      }
    }

    // All dimension columns should be dictionary encoded.
    // All dimension columns must be single value
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      String dimension = fieldSpec.getName();
      if (noDictionaryColumns.contains(dimension)) {
        _logger
            .warn("Metrics aggregation cannot be turned ON in presence of no-dictionary dimensions, eg: {}", dimension);
        _aggregateMetrics = false;
        break;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of multi-value dimension columns, eg: {}",
            dimension);
        _aggregateMetrics = false;
        break;
      }
    }

    // Time column should be dictionary encoded.
    String timeColumn = _schema.getTimeColumnName();
    if (noDictionaryColumns.contains(timeColumn)) {
      _logger
          .warn("Metrics aggregation cannot be turned ON in presence of no-dictionary time column, eg: {}", timeColumn);
      _aggregateMetrics = false;
    }

    if (!_aggregateMetrics) {
      return null;
    }

    int estimatedRowsToIndex;
    if (_statsHistory.isEmpty()) {
      // Choose estimated rows to index as maxNumRowsPerSegment / EXPECTED_COMPRESSION (1000, to be conservative in size).
      // These are just heuristics at the moment, and can be refined based on experimental results.
      estimatedRowsToIndex = Math.max(config.getCapacity() / EXPECTED_COMPRESSION, MIN_ROWS_TO_INDEX);
    } else {
      estimatedRowsToIndex = Math.max(_statsHistory.getEstimatedRowsToIndex(), MIN_ROWS_TO_INDEX);
    }

    // Compute size of overflow map.
    int maxOverFlowHashSize = Math.max(estimatedRowsToIndex / 1000, MIN_RECORD_ID_MAP_CACHE_SIZE);

    _logger.info("Initializing metrics update: estimatedRowsToIndex:{}, cacheSize:{}", estimatedRowsToIndex,
        maxOverFlowHashSize);
    return new FixedIntArrayOffHeapIdMap(estimatedRowsToIndex, maxOverFlowHashSize, _numKeyColumns, _memoryManager,
        RECORD_ID_MAP);
  }
}
