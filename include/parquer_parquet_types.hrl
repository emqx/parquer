%%
%% Autogenerated by Thrift Compiler (0.21.0)
%%
%% DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
%%

-ifndef(_parquer_parquet_types_included).
-define(_parquer_parquet_types_included, yeah).

-define(Parquer_parquet_Type_BOOLEAN, 0).
-define(Parquer_parquet_Type_INT32, 1).
-define(Parquer_parquet_Type_INT64, 2).
-define(Parquer_parquet_Type_INT96, 3).
-define(Parquer_parquet_Type_FLOAT, 4).
-define(Parquer_parquet_Type_DOUBLE, 5).
-define(Parquer_parquet_Type_BYTE_ARRAY, 6).
-define(Parquer_parquet_Type_FIXED_LEN_BYTE_ARRAY, 7).

-define(Parquer_parquet_ConvertedType_UTF8, 0).
-define(Parquer_parquet_ConvertedType_MAP, 1).
-define(Parquer_parquet_ConvertedType_MAP_KEY_VALUE, 2).
-define(Parquer_parquet_ConvertedType_LIST, 3).
-define(Parquer_parquet_ConvertedType_ENUM, 4).
-define(Parquer_parquet_ConvertedType_DECIMAL, 5).
-define(Parquer_parquet_ConvertedType_DATE, 6).
-define(Parquer_parquet_ConvertedType_TIME_MILLIS, 7).
-define(Parquer_parquet_ConvertedType_TIME_MICROS, 8).
-define(Parquer_parquet_ConvertedType_TIMESTAMP_MILLIS, 9).
-define(Parquer_parquet_ConvertedType_TIMESTAMP_MICROS, 10).
-define(Parquer_parquet_ConvertedType_UINT_8, 11).
-define(Parquer_parquet_ConvertedType_UINT_16, 12).
-define(Parquer_parquet_ConvertedType_UINT_32, 13).
-define(Parquer_parquet_ConvertedType_UINT_64, 14).
-define(Parquer_parquet_ConvertedType_INT_8, 15).
-define(Parquer_parquet_ConvertedType_INT_16, 16).
-define(Parquer_parquet_ConvertedType_INT_32, 17).
-define(Parquer_parquet_ConvertedType_INT_64, 18).
-define(Parquer_parquet_ConvertedType_JSON, 19).
-define(Parquer_parquet_ConvertedType_BSON, 20).
-define(Parquer_parquet_ConvertedType_INTERVAL, 21).

-define(Parquer_parquet_FieldRepetitionType_REQUIRED, 0).
-define(Parquer_parquet_FieldRepetitionType_OPTIONAL, 1).
-define(Parquer_parquet_FieldRepetitionType_REPEATED, 2).

-define(Parquer_parquet_EdgeInterpolationAlgorithm_SPHERICAL, 0).
-define(Parquer_parquet_EdgeInterpolationAlgorithm_VINCENTY, 1).
-define(Parquer_parquet_EdgeInterpolationAlgorithm_THOMAS, 2).
-define(Parquer_parquet_EdgeInterpolationAlgorithm_ANDOYER, 3).
-define(Parquer_parquet_EdgeInterpolationAlgorithm_KARNEY, 4).

-define(Parquer_parquet_Encoding_PLAIN, 0).
-define(Parquer_parquet_Encoding_PLAIN_DICTIONARY, 2).
-define(Parquer_parquet_Encoding_RLE, 3).
-define(Parquer_parquet_Encoding_BIT_PACKED, 4).
-define(Parquer_parquet_Encoding_DELTA_BINARY_PACKED, 5).
-define(Parquer_parquet_Encoding_DELTA_LENGTH_BYTE_ARRAY, 6).
-define(Parquer_parquet_Encoding_DELTA_BYTE_ARRAY, 7).
-define(Parquer_parquet_Encoding_RLE_DICTIONARY, 8).
-define(Parquer_parquet_Encoding_BYTE_STREAM_SPLIT, 9).

-define(Parquer_parquet_CompressionCodec_UNCOMPRESSED, 0).
-define(Parquer_parquet_CompressionCodec_SNAPPY, 1).
-define(Parquer_parquet_CompressionCodec_GZIP, 2).
-define(Parquer_parquet_CompressionCodec_LZO, 3).
-define(Parquer_parquet_CompressionCodec_BROTLI, 4).
-define(Parquer_parquet_CompressionCodec_LZ4, 5).
-define(Parquer_parquet_CompressionCodec_ZSTD, 6).
-define(Parquer_parquet_CompressionCodec_LZ4_RAW, 7).

-define(Parquer_parquet_PageType_DATA_PAGE, 0).
-define(Parquer_parquet_PageType_INDEX_PAGE, 1).
-define(Parquer_parquet_PageType_DICTIONARY_PAGE, 2).
-define(Parquer_parquet_PageType_DATA_PAGE_V2, 3).

-define(Parquer_parquet_BoundaryOrder_UNORDERED, 0).
-define(Parquer_parquet_BoundaryOrder_ASCENDING, 1).
-define(Parquer_parquet_BoundaryOrder_DESCENDING, 2).

%% struct 'sizeStatistics'

-record('sizeStatistics', {
    'unencoded_byte_array_data_bytes' :: integer() | 'undefined',
    'repetition_level_histogram' :: list() | 'undefined',
    'definition_level_histogram' :: list() | 'undefined'
}).
-type 'sizeStatistics'() :: #'sizeStatistics'{}.

%% struct 'boundingBox'

-record('boundingBox', {
    'xmin' :: float(),
    'xmax' :: float(),
    'ymin' :: float(),
    'ymax' :: float(),
    'zmin' :: float() | 'undefined',
    'zmax' :: float() | 'undefined',
    'mmin' :: float() | 'undefined',
    'mmax' :: float() | 'undefined'
}).
-type 'boundingBox'() :: #'boundingBox'{}.

%% struct 'geospatialStatistics'

-record('geospatialStatistics', {
    'bbox' :: 'boundingBox'() | 'undefined',
    'geospatial_types' :: list() | 'undefined'
}).
-type 'geospatialStatistics'() :: #'geospatialStatistics'{}.

%% struct 'statistics'

-record('statistics', {
    'max' :: string() | binary() | 'undefined',
    'min' :: string() | binary() | 'undefined',
    'null_count' :: integer() | 'undefined',
    'distinct_count' :: integer() | 'undefined',
    'max_value' :: string() | binary() | 'undefined',
    'min_value' :: string() | binary() | 'undefined',
    'is_max_value_exact' :: boolean() | 'undefined',
    'is_min_value_exact' :: boolean() | 'undefined'
}).
-type 'statistics'() :: #'statistics'{}.

%% struct 'stringType'

-record('stringType', {}).
-type 'stringType'() :: #'stringType'{}.

%% struct 'uUIDType'

-record('uUIDType', {}).
-type 'uUIDType'() :: #'uUIDType'{}.

%% struct 'mapType'

-record('mapType', {}).
-type 'mapType'() :: #'mapType'{}.

%% struct 'listType'

-record('listType', {}).
-type 'listType'() :: #'listType'{}.

%% struct 'enumType'

-record('enumType', {}).
-type 'enumType'() :: #'enumType'{}.

%% struct 'dateType'

-record('dateType', {}).
-type 'dateType'() :: #'dateType'{}.

%% struct 'float16Type'

-record('float16Type', {}).
-type 'float16Type'() :: #'float16Type'{}.

%% struct 'nullType'

-record('nullType', {}).
-type 'nullType'() :: #'nullType'{}.

%% struct 'decimalType'

-record('decimalType', {
    'scale' :: integer(),
    'precision' :: integer()
}).
-type 'decimalType'() :: #'decimalType'{}.

%% struct 'milliSeconds'

-record('milliSeconds', {}).
-type 'milliSeconds'() :: #'milliSeconds'{}.

%% struct 'microSeconds'

-record('microSeconds', {}).
-type 'microSeconds'() :: #'microSeconds'{}.

%% struct 'nanoSeconds'

-record('nanoSeconds', {}).
-type 'nanoSeconds'() :: #'nanoSeconds'{}.

%% struct 'timeUnit'

-record('timeUnit', {
    'mILLIS' :: 'milliSeconds'() | 'undefined',
    'mICROS' :: 'microSeconds'() | 'undefined',
    'nANOS' :: 'nanoSeconds'() | 'undefined'
}).
-type 'timeUnit'() :: #'timeUnit'{}.

%% struct 'timestampType'

-record('timestampType', {
    'isAdjustedToUTC' :: boolean(),
    'unit' = #'timeUnit'{} :: 'timeUnit'()
}).
-type 'timestampType'() :: #'timestampType'{}.

%% struct 'timeType'

-record('timeType', {
    'isAdjustedToUTC' :: boolean(),
    'unit' = #'timeUnit'{} :: 'timeUnit'()
}).
-type 'timeType'() :: #'timeType'{}.

%% struct 'intType'

-record('intType', {
    'bitWidth' :: integer(),
    'isSigned' :: boolean()
}).
-type 'intType'() :: #'intType'{}.

%% struct 'jsonType'

-record('jsonType', {}).
-type 'jsonType'() :: #'jsonType'{}.

%% struct 'bsonType'

-record('bsonType', {}).
-type 'bsonType'() :: #'bsonType'{}.

%% struct 'variantType'

-record('variantType', {'specification_version' :: integer() | 'undefined'}).
-type 'variantType'() :: #'variantType'{}.

%% struct 'geometryType'

-record('geometryType', {'crs' :: string() | binary() | 'undefined'}).
-type 'geometryType'() :: #'geometryType'{}.

%% struct 'geographyType'

-record('geographyType', {
    'crs' :: string() | binary() | 'undefined',
    'algorithm' :: integer() | 'undefined'
}).
-type 'geographyType'() :: #'geographyType'{}.

%% struct 'logicalType'

-record('logicalType', {
    'sTRING' :: 'stringType'() | 'undefined',
    'mAP' :: 'mapType'() | 'undefined',
    'lIST' :: 'listType'() | 'undefined',
    'eNUM' :: 'enumType'() | 'undefined',
    'dECIMAL' :: 'decimalType'() | 'undefined',
    'dATE' :: 'dateType'() | 'undefined',
    'tIME' :: 'timeType'() | 'undefined',
    'tIMESTAMP' :: 'timestampType'() | 'undefined',
    'iNTEGER' :: 'intType'() | 'undefined',
    'uNKNOWN' :: 'nullType'() | 'undefined',
    'jSON' :: 'jsonType'() | 'undefined',
    'bSON' :: 'bsonType'() | 'undefined',
    'uUID' :: 'uUIDType'() | 'undefined',
    'fLOAT16' :: 'float16Type'() | 'undefined',
    'vARIANT' :: 'variantType'() | 'undefined',
    'gEOMETRY' :: 'geometryType'() | 'undefined',
    'gEOGRAPHY' :: 'geographyType'() | 'undefined'
}).
-type 'logicalType'() :: #'logicalType'{}.

%% struct 'schemaElement'

-record('schemaElement', {
    'type' :: integer() | 'undefined',
    'type_length' :: integer() | 'undefined',
    'repetition_type' :: integer() | 'undefined',
    'name' :: string() | binary(),
    'num_children' :: integer() | 'undefined',
    'converted_type' :: integer() | 'undefined',
    'scale' :: integer() | 'undefined',
    'precision' :: integer() | 'undefined',
    'field_id' :: integer() | 'undefined',
    'logicalType' :: 'logicalType'() | 'undefined'
}).
-type 'schemaElement'() :: #'schemaElement'{}.

%% struct 'dataPageHeader'

-record('dataPageHeader', {
    'num_values' :: integer(),
    'encoding' :: integer(),
    'definition_level_encoding' :: integer(),
    'repetition_level_encoding' :: integer(),
    'statistics' :: 'statistics'() | 'undefined'
}).
-type 'dataPageHeader'() :: #'dataPageHeader'{}.

%% struct 'indexPageHeader'

-record('indexPageHeader', {}).
-type 'indexPageHeader'() :: #'indexPageHeader'{}.

%% struct 'dictionaryPageHeader'

-record('dictionaryPageHeader', {
    'num_values' :: integer(),
    'encoding' :: integer(),
    'is_sorted' :: boolean() | 'undefined'
}).
-type 'dictionaryPageHeader'() :: #'dictionaryPageHeader'{}.

%% struct 'dataPageHeaderV2'

-record('dataPageHeaderV2', {
    'num_values' :: integer(),
    'num_nulls' :: integer(),
    'num_rows' :: integer(),
    'encoding' :: integer(),
    'definition_levels_byte_length' :: integer(),
    'repetition_levels_byte_length' :: integer(),
    'is_compressed' = true :: boolean() | 'undefined',
    'statistics' :: 'statistics'() | 'undefined'
}).
-type 'dataPageHeaderV2'() :: #'dataPageHeaderV2'{}.

%% struct 'splitBlockAlgorithm'

-record('splitBlockAlgorithm', {}).
-type 'splitBlockAlgorithm'() :: #'splitBlockAlgorithm'{}.

%% struct 'bloomFilterAlgorithm'

-record('bloomFilterAlgorithm', {'bLOCK' :: 'splitBlockAlgorithm'() | 'undefined'}).
-type 'bloomFilterAlgorithm'() :: #'bloomFilterAlgorithm'{}.

%% struct 'xxHash'

-record('xxHash', {}).
-type 'xxHash'() :: #'xxHash'{}.

%% struct 'bloomFilterHash'

-record('bloomFilterHash', {'xXHASH' :: 'xxHash'() | 'undefined'}).
-type 'bloomFilterHash'() :: #'bloomFilterHash'{}.

%% struct 'uncompressed'

-record('uncompressed', {}).
-type 'uncompressed'() :: #'uncompressed'{}.

%% struct 'bloomFilterCompression'

-record('bloomFilterCompression', {'uNCOMPRESSED' :: 'uncompressed'() | 'undefined'}).
-type 'bloomFilterCompression'() :: #'bloomFilterCompression'{}.

%% struct 'bloomFilterHeader'

-record('bloomFilterHeader', {
    'numBytes' :: integer(),
    'algorithm' = #'bloomFilterAlgorithm'{} :: 'bloomFilterAlgorithm'(),
    'hash' = #'bloomFilterHash'{} :: 'bloomFilterHash'(),
    'compression' = #'bloomFilterCompression'{} :: 'bloomFilterCompression'()
}).
-type 'bloomFilterHeader'() :: #'bloomFilterHeader'{}.

%% struct 'pageHeader'

-record('pageHeader', {
    'type' :: integer(),
    'uncompressed_page_size' :: integer(),
    'compressed_page_size' :: integer(),
    'crc' :: integer() | 'undefined',
    'data_page_header' :: 'dataPageHeader'() | 'undefined',
    'index_page_header' :: 'indexPageHeader'() | 'undefined',
    'dictionary_page_header' :: 'dictionaryPageHeader'() | 'undefined',
    'data_page_header_v2' :: 'dataPageHeaderV2'() | 'undefined'
}).
-type 'pageHeader'() :: #'pageHeader'{}.

%% struct 'keyValue'

-record('keyValue', {
    'key' :: string() | binary(),
    'value' :: string() | binary() | 'undefined'
}).
-type 'keyValue'() :: #'keyValue'{}.

%% struct 'sortingColumn'

-record('sortingColumn', {
    'column_idx' :: integer(),
    'descending' :: boolean(),
    'nulls_first' :: boolean()
}).
-type 'sortingColumn'() :: #'sortingColumn'{}.

%% struct 'pageEncodingStats'

-record('pageEncodingStats', {
    'page_type' :: integer(),
    'encoding' :: integer(),
    'count' :: integer()
}).
-type 'pageEncodingStats'() :: #'pageEncodingStats'{}.

%% struct 'columnMetaData'

-record('columnMetaData', {
    'type' :: integer(),
    'encodings' = [] :: list(),
    'path_in_schema' = [] :: list(),
    'codec' :: integer(),
    'num_values' :: integer(),
    'total_uncompressed_size' :: integer(),
    'total_compressed_size' :: integer(),
    'key_value_metadata' :: list() | 'undefined',
    'data_page_offset' :: integer(),
    'index_page_offset' :: integer() | 'undefined',
    'dictionary_page_offset' :: integer() | 'undefined',
    'statistics' :: 'statistics'() | 'undefined',
    'encoding_stats' :: list() | 'undefined',
    'bloom_filter_offset' :: integer() | 'undefined',
    'bloom_filter_length' :: integer() | 'undefined',
    'size_statistics' :: 'sizeStatistics'() | 'undefined',
    'geospatial_statistics' :: 'geospatialStatistics'() | 'undefined'
}).
-type 'columnMetaData'() :: #'columnMetaData'{}.

%% struct 'encryptionWithFooterKey'

-record('encryptionWithFooterKey', {}).
-type 'encryptionWithFooterKey'() :: #'encryptionWithFooterKey'{}.

%% struct 'encryptionWithColumnKey'

-record('encryptionWithColumnKey', {
    'path_in_schema' = [] :: list(),
    'key_metadata' :: string() | binary() | 'undefined'
}).
-type 'encryptionWithColumnKey'() :: #'encryptionWithColumnKey'{}.

%% struct 'columnCryptoMetaData'

-record('columnCryptoMetaData', {
    'eNCRYPTION_WITH_FOOTER_KEY' :: 'encryptionWithFooterKey'() | 'undefined',
    'eNCRYPTION_WITH_COLUMN_KEY' :: 'encryptionWithColumnKey'() | 'undefined'
}).
-type 'columnCryptoMetaData'() :: #'columnCryptoMetaData'{}.

%% struct 'columnChunk'

-record('columnChunk', {
    'file_path' :: string() | binary() | 'undefined',
    'file_offset' = 0 :: integer(),
    'meta_data' :: 'columnMetaData'() | 'undefined',
    'offset_index_offset' :: integer() | 'undefined',
    'offset_index_length' :: integer() | 'undefined',
    'column_index_offset' :: integer() | 'undefined',
    'column_index_length' :: integer() | 'undefined',
    'crypto_metadata' :: 'columnCryptoMetaData'() | 'undefined',
    'encrypted_column_metadata' :: string() | binary() | 'undefined'
}).
-type 'columnChunk'() :: #'columnChunk'{}.

%% struct 'rowGroup'

-record('rowGroup', {
    'columns' = [] :: list(),
    'total_byte_size' :: integer(),
    'num_rows' :: integer(),
    'sorting_columns' :: list() | 'undefined',
    'file_offset' :: integer() | 'undefined',
    'total_compressed_size' :: integer() | 'undefined',
    'ordinal' :: integer() | 'undefined'
}).
-type 'rowGroup'() :: #'rowGroup'{}.

%% struct 'typeDefinedOrder'

-record('typeDefinedOrder', {}).
-type 'typeDefinedOrder'() :: #'typeDefinedOrder'{}.

%% struct 'columnOrder'

-record('columnOrder', {'tYPE_ORDER' :: 'typeDefinedOrder'() | 'undefined'}).
-type 'columnOrder'() :: #'columnOrder'{}.

%% struct 'pageLocation'

-record('pageLocation', {
    'offset' :: integer(),
    'compressed_page_size' :: integer(),
    'first_row_index' :: integer()
}).
-type 'pageLocation'() :: #'pageLocation'{}.

%% struct 'offsetIndex'

-record('offsetIndex', {
    'page_locations' = [] :: list(),
    'unencoded_byte_array_data_bytes' :: list() | 'undefined'
}).
-type 'offsetIndex'() :: #'offsetIndex'{}.

%% struct 'columnIndex'

-record('columnIndex', {
    'null_pages' = [] :: list(),
    'min_values' = [] :: list(),
    'max_values' = [] :: list(),
    'boundary_order' :: integer(),
    'null_counts' :: list() | 'undefined',
    'repetition_level_histograms' :: list() | 'undefined',
    'definition_level_histograms' :: list() | 'undefined'
}).
-type 'columnIndex'() :: #'columnIndex'{}.

%% struct 'aesGcmV1'

-record('aesGcmV1', {
    'aad_prefix' :: string() | binary() | 'undefined',
    'aad_file_unique' :: string() | binary() | 'undefined',
    'supply_aad_prefix' :: boolean() | 'undefined'
}).
-type 'aesGcmV1'() :: #'aesGcmV1'{}.

%% struct 'aesGcmCtrV1'

-record('aesGcmCtrV1', {
    'aad_prefix' :: string() | binary() | 'undefined',
    'aad_file_unique' :: string() | binary() | 'undefined',
    'supply_aad_prefix' :: boolean() | 'undefined'
}).
-type 'aesGcmCtrV1'() :: #'aesGcmCtrV1'{}.

%% struct 'encryptionAlgorithm'

-record('encryptionAlgorithm', {
    'aES_GCM_V1' :: 'aesGcmV1'() | 'undefined',
    'aES_GCM_CTR_V1' :: 'aesGcmCtrV1'() | 'undefined'
}).
-type 'encryptionAlgorithm'() :: #'encryptionAlgorithm'{}.

%% struct 'fileMetaData'

-record('fileMetaData', {
    'version' :: integer(),
    'schema' = [] :: list(),
    'num_rows' :: integer(),
    'row_groups' = [] :: list(),
    'key_value_metadata' :: list() | 'undefined',
    'created_by' :: string() | binary() | 'undefined',
    'column_orders' :: list() | 'undefined',
    'encryption_algorithm' :: 'encryptionAlgorithm'() | 'undefined',
    'footer_signing_key_metadata' :: string() | binary() | 'undefined'
}).
-type 'fileMetaData'() :: #'fileMetaData'{}.

%% struct 'fileCryptoMetaData'

-record('fileCryptoMetaData', {
    'encryption_algorithm' = #'encryptionAlgorithm'{} :: 'encryptionAlgorithm'(),
    'key_metadata' :: string() | binary() | 'undefined'
}).
-type 'fileCryptoMetaData'() :: #'fileCryptoMetaData'{}.

-endif.
