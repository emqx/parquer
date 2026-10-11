%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(parquer_writer).

%% API
-export([
    new/2,
    write/2,
    write_many/2,
    close/1
]).

%% Debugging only
-export([inspect/1]).

-export_type([t/0, write_metadata/0]).

-include("parquer.hrl").
-include("parquer_thrift.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% 128 mb
-define(DEFAULT_MAX_ROW_GROUP_BYTES, 128 * 1024 * 1024).

-define(DEFAULT_COMPRESSION, ?COMPRESSION_ZSTD).
-define(DEFAULT_COMPRESSION_OPTS, #{}).
-define(DEFAULT_DATA_HEADER_VSN, 1).

-define(IS_VALID_DATA_HEADER_VSN(VSN), ((VSN == 1) orelse (VSN == 2))).

-ifdef(TEST).
-define(MAX_DICT_SIZE, 100).
-else.
%% 2 ** 32 - 1
-define(MAX_DICT_SIZE, 4294967295).
-endif.

-define(compressed, compressed).
-define(compressed_size, compressed_size).
-define(concrete_dictionary, concrete_dictionary).
-define(data_page_header_version, data_page_header_version).
-define(def_level_bin, def_level_bin).
-define(enable_dictionary, enable_dictionary).
-define(max_row_group_bytes, max_row_group_bytes).
-define(num_dict_keys, num_dict_keys).
-define(path, path).
-define(rep_level_bin, rep_level_bin).
-define(uncompressed, uncompressed).
-define(uncompressed_size, uncompressed_size).

%% State
-record(writer, {
    schema,
    closed_row_groups = [],
    columns,
    has_data = false,
    num_rows = 0,
    total_num_rows = 0,
    offset = 0,
    opts
}).

%% Column state
-record(c, {
    id,
    name,
    path,
    repetition,
    primitive_type,
    type_length,
    data_page_version,
    max,
    min,
    max_definition_level,
    max_repetition_level,
    num_values = 0,
    num_nulls = 0,
    num_rows = 0,
    %% N.B. these are in reverse order
    definition_levels = [],
    %% N.B. these are in reverse order
    repetition_levels = [],
    data,
    zipper
}).

%% Other values
-record(data, {
    byte_size = 0,
    enable_dictionary,
    dictionary = #{},
    values = []
}).

-record(write_meta, {
    column,
    num_rows,
    total_compressed_size,
    total_uncompressed_size,
    dict_page_header_size,
    dict_uncompressed_size,
    dict_compressed_size,
    data_page_header_size,
    data_uncompressed_size,
    data_compressed_size
}).

-opaque t() :: #writer{}.

-doc """

## Options

  * `data_page_header_version` - which data page version to use.
    Valid values: `1` | `2`.
    Defaults to `1`.

  * `default_compression` - the compression algorithm to be used when it's not specified
    per column, along with its options map.
    Valid types: `none` | `snappy` | `zstd`.
    Defaults to {`zstd`, #{}}.

  * `enable_dictionary` - whether to use dictionary encoding.  Can be set for all columns
    as a single boolean, or individually by providing a map of column paths with segments
    separated by dots (e.g.: `<<"my.nested.column">>`) to boolean values.  Only applies to
    columns that are not boolean.  Default is `true` (enabled for all columns).

  * `max_row_group_bytes` - when the buffered data size surpasses this value
    (approximately), a new row group will be emitted.  Defaults to 128 MB.

""".
-type writer_opts() :: #{
    ?default_compression => compression(),
    ?data_page_header_version => 1..2,
    any() => term()
}.
-type data() :: parquer_zipper:data().
-type compression() ::
    {?COMPRESSION_NONE, compression_none_opts()}
    | {?COMPRESSION_SNAPPY, compression_snappy_opts()}
    | {?COMPRESSION_ZSTD, compression_zstd_opts()}.

-type compression_none_opts() :: #{}.
-type compression_snappy_opts() :: #{}.
-type compression_zstd_opts() :: #{}.

-type write_metadata() :: #{
    atom() => term()
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec new(map(), writer_opts()) -> t().
new(Schema, #{} = Opts0) ->
    DefaultOpts = #{
        ?default_compression => {?DEFAULT_COMPRESSION, ?DEFAULT_COMPRESSION_OPTS},
        ?enable_dictionary => true,
        ?data_page_header_version => 1,
        ?max_row_group_bytes => ?DEFAULT_MAX_ROW_GROUP_BYTES
    },
    Opts = maps:merge(DefaultOpts, Opts0),
    FlatSchema = parquer_schema:flatten(Schema),
    Columns = initialize_columns(FlatSchema, Opts),
    #writer{schema = FlatSchema, columns = Columns, opts = Opts}.

-spec write_many(t(), [data()]) -> {iodata(), t()}.
write_many(#writer{} = Writer0, Records) when is_list(Records) ->
    NumRecords = length(Records),
    Cols = lists:map(
        fun(C) -> append_to_column(C, Records) end,
        Writer0#writer.columns
    ),
    Writer1 = Writer0#writer{
        has_data = true,
        num_rows = Writer0#writer.num_rows + NumRecords,
        total_num_rows = Writer0#writer.total_num_rows + NumRecords,
        columns = Cols
    },
    maybe_emit_row_group(Writer1).

-spec write(t(), data()) -> {iodata(), t()}.
write(#writer{} = Writer0, Record) ->
    Cols = lists:map(
        fun(C) -> append_to_column(C, [Record]) end,
        Writer0#writer.columns
    ),
    Writer1 = Writer0#writer{
        has_data = true,
        num_rows = Writer0#writer.num_rows + 1,
        total_num_rows = Writer0#writer.total_num_rows + 1,
        columns = Cols
    },
    maybe_emit_row_group(Writer1).

-spec close(t()) -> {iodata(), write_metadata()}.
close(#writer{} = Writer) ->
    do_close(Writer).

%%------------------------------------------------------------------------------
%% Debugging only
%%------------------------------------------------------------------------------

inspect(#writer{} = W) ->
    #{
        schema => W#writer.schema,
        closed_row_groups => lists:reverse(W#writer.closed_row_groups),
        columns => lists:map(fun inspect/1, W#writer.columns),
        has_data => W#writer.has_data,
        num_rows => W#writer.num_rows,
        offset => W#writer.offset,
        opts => W#writer.opts
    };
inspect(#c{} = C) ->
    #{
        path => C#c.path,
        repetition => C#c.repetition,
        primitive_type => C#c.primitive_type,
        data_page_header_version => C#c.data_page_version,
        max => C#c.max,
        min => C#c.min,
        max_definition_level => C#c.max_definition_level,
        max_repetition_level => C#c.max_repetition_level,
        num_values => C#c.num_values,
        num_nulls => C#c.num_nulls,
        num_rows => C#c.num_rows,
        definition_levels => lists:reverse(C#c.definition_levels),
        repetition_levels => lists:reverse(C#c.repetition_levels),
        data => inspect(C#c.data)
    };
inspect(#data{} = D) ->
    #{
        byte_size => D#data.byte_size,
        enable_dictionary => D#data.enable_dictionary,
        dictionary => D#data.dictionary,
        values => lists:reverse(D#data.values)
    }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

initialize_columns(FlatSchema, Opts) ->
    lists:filtermap(
        fun(ColSc) ->
            case parquer_schema:is_leaf(ColSc) of
                false ->
                    false;
                true ->
                    {true, initial_column_state(ColSc, Opts)}
            end
        end,
        FlatSchema
    ).

initial_column_state(LeafColumnSchema, Opts) ->
    #{
        ?path := [_Root | Path],
        ?name := Name,
        ?repetition := Repetition,
        ?primitive_type := PrimitiveType,
        ?max_definition_level := MaxDefLevel,
        ?max_repetition_level := MaxRepLevel
    } = LeafColumnSchema,
    TypeLength = maps:get(?type_length, LeafColumnSchema, ?undefined),
    KeyRepetitions = Path ++ [{Name, Repetition}],
    {KeyPath, _} = lists:unzip(KeyRepetitions),
    DataPageVersion = data_page_header_version(KeyPath, Opts),
    #c{
        id = maps:get(?id, LeafColumnSchema, ?undefined),
        name = Name,
        path = KeyRepetitions,
        repetition = Repetition,
        primitive_type = PrimitiveType,
        type_length = TypeLength,
        data_page_version = DataPageVersion,
        max = undefined,
        min = undefined,
        max_repetition_level = MaxRepLevel,
        max_definition_level = MaxDefLevel,
        repetition_levels = [],
        definition_levels = [],
        data = init_data(PrimitiveType, KeyPath, Opts),
        zipper = parquer_zipper:new(KeyRepetitions, [])
    }.

init_data(?BOOLEAN, _KeyPath, _Opts) ->
    #data{
        enable_dictionary = false,
        dictionary = #{},
        values = []
    };
init_data(_PrimitiveType, KeyPath, Opts) ->
    IsDictEnabled = is_dictionary_enabled(KeyPath, Opts),
    #data{
        enable_dictionary = IsDictEnabled,
        dictionary = #{},
        values = []
    }.

append_to_column(#c{} = Col0, []) ->
    Col0;
append_to_column(#c{} = Col0, [Record | Rest]) ->
    #c{zipper = Zipper0} = Col0,
    Zipper = parquer_zipper:reset(Zipper0, Record),
    Col1 = Col0#c{num_rows = Col0#c.num_rows + 1},
    do_append_to_column(Col1, parquer_zipper:next_read(Zipper), Rest).

-compile({inline, [do_append_to_column/3]}).
do_append_to_column(Col0, ?undefined, Rest) ->
    append_to_column(Col0, Rest);
do_append_to_column(Col0, {{RepLevel, DefLevel, ?undefined}, Zipper0}, Rest) ->
    Col1 = Col0#c{
        num_values = Col0#c.num_values + 1,
        num_nulls = Col0#c.num_nulls + 1
    },
    Col2 = append_definition_level(Col1, DefLevel),
    Col3 = append_repetition_level(Col2, RepLevel),
    do_append_to_column(Col3, parquer_zipper:next_read(Zipper0), Rest);
do_append_to_column(Col0, {{RepLevel, DefLevel, Val}, Zipper0}, Rest) ->
    Col1 = Col0#c{
        num_values = Col0#c.num_values + 1
    },
    Col2 = append_definition_level(Col1, DefLevel),
    Col3 = append_repetition_level(Col2, RepLevel),
    Col4 = append_datum(Col3, Val),
    do_append_to_column(Col4, parquer_zipper:next_read(Zipper0), Rest).

append_datum(#c{data = #data{enable_dictionary = true}} = C0, Datum) ->
    #data{values = Values0} = Data0 = C0#c.data,
    Data1 = ensure_key(Data0, Datum, C0),
    Values1 = [Datum | Values0],
    Data2 = Data1#data{values = Values1},
    C0#c{data = Data2};
append_datum(#c{} = C0, Datum) ->
    #data{values = Values0} = Data0 = C0#c.data,
    Values1 = [Datum | Values0],
    Data1 = Data0#data{values = Values1},
    C0#c{data = Data1}.

append_definition_level(#c{max_definition_level = 0} = Col0, _DefLevel) ->
    Col0;
append_definition_level(#c{definition_levels = DefinitionLevels0} = Col0, DefLevel) ->
    Col0#c{definition_levels = [DefLevel | DefinitionLevels0]}.

append_repetition_level(#c{max_repetition_level = 0} = Col0, _RepLevel) ->
    Col0;
append_repetition_level(#c{repetition_levels = RepetitionLevels0} = Col0, RepLevel) ->
    Col0#c{repetition_levels = [RepLevel | RepetitionLevels0]}.

ensure_key(#data{dictionary = Dict} = D, Datum, _C) when is_map_key(Datum, Dict) ->
    D;
ensure_key(#data{dictionary = Dict} = D0, _Datum, _C) when map_size(Dict) >= ?MAX_DICT_SIZE ->
    %% Fallback to plain encoding
    D0#data{
        dictionary = #{},
        enable_dictionary = false
    };
ensure_key(#data{dictionary = Dict0, byte_size = ByteSize0} = D0, Datum, C) ->
    Dict = Dict0#{Datum => true},
    ByteSize = ByteSize0 + estimate_datum_byte_size(Datum, C),
    D0#data{
        dictionary = Dict,
        byte_size = ByteSize
    }.

estimate_datum_byte_size(Bin, #c{primitive_type = ?BYTE_ARRAY}) when is_binary(Bin) ->
    byte_size(Bin);
estimate_datum_byte_size(Bin, #c{primitive_type = ?FIXED_LEN_BYTE_ARRAY, type_length = Len}) when
    is_binary(Bin), is_integer(Len)
->
    Len;
estimate_datum_byte_size(Int, #c{primitive_type = ?INT32}) when is_integer(Int) ->
    4;
estimate_datum_byte_size(Int, #c{primitive_type = ?INT64}) when is_integer(Int) ->
    8;
estimate_datum_byte_size(Int, #c{primitive_type = ?INT96}) when is_integer(Int) ->
    12;
estimate_datum_byte_size(Num, #c{primitive_type = ?FLOAT}) when is_number(Num) ->
    4;
estimate_datum_byte_size(Num, #c{primitive_type = ?DOUBLE}) when is_number(Num) ->
    8.

%% todo: how to improve?
estimate_byte_size(#c{data = #data{byte_size = ByteSize}}) ->
    ByteSize.

-spec do_close(t()) -> {iodata(), write_metadata()}.
do_close(#writer{} = Writer0) ->
    {IOData0, Writer1} =
        case Writer0#writer.has_data of
            true ->
                close_row_group(Writer0);
            false ->
                {[], Writer0}
        end,
    RowGroups = lists:reverse(Writer1#writer.closed_row_groups),
    FMD = parquer_thrift_utils:file_metadata(#{
        %% todo: check what to put here
        ?version => 1,
        ?schema => schema_to_thrift(Writer1),
        ?num_rows => Writer1#writer.total_num_rows,
        ?row_groups => RowGroups
    }),
    FMDBin = parquer_thrift_utils:serialize(FMD),
    FMDBinSize = iolist_size(FMDBin),
    %% For future use; maybe return the FMD here.
    WriteMetaOut = #{},
    {[IOData0, FMDBin, encode_data_size(FMDBinSize), ?MAGIC_NOT_ENCRYPTED], WriteMetaOut}.

-spec maybe_emit_row_group(t()) -> {iodata(), t()}.
maybe_emit_row_group(#writer{} = Writer0) ->
    EstimatedByteSize =
        lists:foldl(
            fun(C, Acc) -> estimate_byte_size(C) + Acc end,
            0,
            Writer0#writer.columns
        ),
    MaxRowGroupBytes = max_row_group_bytes(Writer0),
    case EstimatedByteSize >= MaxRowGroupBytes of
        true ->
            close_row_group(Writer0);
        false ->
            {[], Writer0}
    end.

close_row_group(#writer{} = Writer0) ->
    {MMagic, Writer1} = maybe_emit_magic(Writer0),
    {IODataMetaAndChunks, Writer2} =
        lists:mapfoldl(
            fun(Col, WriterAcc0) ->
                {IOData, WriteMeta} = serialize_column(Col, WriterAcc0),
                {Chunk, WriterAcc1} = mk_column_chunk(WriteMeta, Col, WriterAcc0),
                {{IOData, WriteMeta, Chunk}, WriterAcc1}
            end,
            Writer1,
            Writer1#writer.columns
        ),
    {IOData, WriteMetas, ColumnChunks} = lists:unzip3(IODataMetaAndChunks),
    {TotalUncompSize, TotalCompSize} =
        lists:foldl(
            fun(WriteMeta, {UncompAcc, CompAcc}) ->
                {
                    UncompAcc + WriteMeta#write_meta.total_uncompressed_size,
                    CompAcc + WriteMeta#write_meta.total_compressed_size
                }
            end,
            {0, 0},
            WriteMetas
        ),
    RowGroup = parquer_thrift_utils:row_group(#{
        ?column_chunks => ColumnChunks,
        ?total_uncompressed_size => TotalUncompSize,
        ?total_compressed_size => TotalCompSize,
        ?num_rows => Writer2#writer.num_rows
    }),
    Writer3 = Writer2#writer{
        has_data = false,
        num_rows = 0,
        columns = initialize_columns(Writer2#writer.schema, Writer2#writer.opts),
        closed_row_groups = [RowGroup | Writer2#writer.closed_row_groups]
    },
    {[MMagic, IOData], Writer3}.

maybe_emit_magic(#writer{offset = 0} = Writer0) ->
    %% File just started.
    emit_magic(Writer0);
maybe_emit_magic(#writer{} = Writer) ->
    {[], Writer}.

emit_magic(#writer{} = Writer0) ->
    Writer = Writer0#writer{offset = byte_size(?MAGIC_NOT_ENCRYPTED)},
    {?MAGIC_NOT_ENCRYPTED, Writer}.

serialize_column(#c{primitive_type = ?BOOLEAN} = C, #writer{} = W) ->
    %% Serialize repetition levels, definition levels, data.
    #c{data = #data{values = RevValues}} = C,
    RepLevelBin = serialize_repetition_levels(C),
    DefLevelBin = serialize_definition_levels(C),
    DataBitWidth = 1,
    DataEncoder0 = parquer_rle_bp_hybrid_encoder:new(DataBitWidth),
    DataEncoder1 =
        lists:foldl(
            fun
                (true, Acc) ->
                    parquer_rle_bp_hybrid_encoder:write_int(Acc, 1);
                (false, Acc) ->
                    parquer_rle_bp_hybrid_encoder:write_int(Acc, 0)
            end,
            DataEncoder0,
            lists:reverse(RevValues)
        ),
    DataBin0 = parquer_rle_bp_hybrid_encoder:to_bytes(DataEncoder1),
    DataBin = [encode_data_size(iolist_size(DataBin0)) | DataBin0],
    {ColDataBinComp, ColDataBin} = compress_data(RepLevelBin, DefLevelBin, DataBin, C, W),
    ColDataBinSize = iolist_size(ColDataBin),
    ColDataBinCompSize = iolist_size(ColDataBinComp),
    DataSerializedInfo = #{
        ?uncompressed => ColDataBin,
        ?compressed => ColDataBinComp,
        ?def_level_bin => DefLevelBin,
        ?rep_level_bin => RepLevelBin,
        ?uncompressed_size => ColDataBinSize,
        ?compressed_size => ColDataBinCompSize
    },
    %% Serialize data page header
    PageHeaderDataBin = serialize_data_page(DataSerializedInfo, C),
    IOData = [PageHeaderDataBin, ColDataBinComp],
    PageHeaderBinSize = iolist_size(PageHeaderDataBin),
    WriteMeta = #write_meta{
        column = write_metadata_column_id(C),
        num_rows = C#c.num_rows,
        total_compressed_size = PageHeaderBinSize + ColDataBinCompSize,
        total_uncompressed_size = PageHeaderBinSize + ColDataBinSize,
        data_page_header_size = PageHeaderBinSize,
        data_uncompressed_size = ColDataBinSize,
        data_compressed_size = ColDataBinCompSize
    },
    {IOData, WriteMeta};
serialize_column(#c{data = #data{enable_dictionary = false}} = C, #writer{} = W) ->
    %% Serialize repetition level, definition level, data. (plain)
    #{
        ?compressed := ColDataBinComp,
        ?def_level_bin := _DefLevelBin,
        ?rep_level_bin := _RepLevelBin,
        ?uncompressed_size := ColDataBinSize,
        ?compressed_size := ColDataBinCompSize
    } = DataSerializedInfo = serialize_data_plain(C, W),
    PageHeaderDataBin = serialize_data_page(DataSerializedInfo, C),
    IOData = [PageHeaderDataBin, ColDataBinComp],
    PageHeaderDataBinSize = iolist_size(PageHeaderDataBin),
    TotalCompSize = PageHeaderDataBinSize + ColDataBinCompSize,
    TotalSize = PageHeaderDataBinSize + ColDataBinSize,
    WriteMeta = #write_meta{
        column = write_metadata_column_id(C),
        num_rows = C#c.num_rows,
        total_compressed_size = TotalCompSize,
        total_uncompressed_size = TotalSize,
        dict_page_header_size = ?undefined,
        dict_uncompressed_size = ?undefined,
        dict_compressed_size = ?undefined,
        data_page_header_size = PageHeaderDataBinSize,
        data_uncompressed_size = ColDataBinSize,
        data_compressed_size = ColDataBinCompSize
    },
    {IOData, WriteMeta};
serialize_column(#c{data = #data{enable_dictionary = true}} = C, #writer{} = W) ->
    %% Serialize dictionary (plain)
    #{
        ?concrete_dictionary := ConcreteDict,
        ?compressed := DictBinComp,
        ?uncompressed_size := DictBinSize,
        ?compressed_size := DictBinCompSize
    } = DictSerializedInfo = serialize_dictionary(C, W),
    %% Serialize dictionary page header
    PageHeaderDictBin = serialize_dictionary_page_header(DictSerializedInfo),
    %% Serialize repetition level, definition level, data. (RLE/BP hybrid)
    #{
        ?compressed := ColDataBinComp,
        ?def_level_bin := _DefLevelBin,
        ?rep_level_bin := _RepLevelBin,
        ?uncompressed_size := ColDataBinSize,
        ?compressed_size := ColDataBinCompSize
    } = DataSerializedInfo = serialize_data_with_dictionary(C, W, ConcreteDict),
    %% Serialize data page header
    PageHeaderDataBin = serialize_data_page(DataSerializedInfo, C),
    IOData = [PageHeaderDictBin, DictBinComp, PageHeaderDataBin, ColDataBinComp],
    PageHeaderDictBinSize = iolist_size(PageHeaderDictBin),
    PageHeaderDataBinSize = iolist_size(PageHeaderDataBin),
    TotalCompSize =
        PageHeaderDictBinSize + DictBinCompSize + PageHeaderDataBinSize + ColDataBinCompSize,
    TotalSize = PageHeaderDictBinSize + DictBinSize + PageHeaderDataBinSize + ColDataBinSize,
    WriteMeta = #write_meta{
        column = write_metadata_column_id(C),
        num_rows = C#c.num_rows,
        total_compressed_size = TotalCompSize,
        total_uncompressed_size = TotalSize,
        dict_page_header_size = PageHeaderDictBinSize,
        dict_uncompressed_size = DictBinSize,
        dict_compressed_size = DictBinCompSize,
        data_page_header_size = PageHeaderDataBinSize,
        data_uncompressed_size = ColDataBinSize,
        data_compressed_size = ColDataBinCompSize
    },
    {IOData, WriteMeta}.

serialize_repetition_levels(#c{max_repetition_level = 0}) ->
    [];
serialize_repetition_levels(#c{} = C) ->
    #c{
        data_page_version = DataPageVersion,
        repetition_levels = RevRepLevels,
        max_repetition_level = MaxRepLevel
    } = C,
    serialize_levels(RevRepLevels, MaxRepLevel, DataPageVersion).

serialize_definition_levels(#c{max_definition_level = 0}) ->
    [];
serialize_definition_levels(#c{} = C) ->
    #c{
        data_page_version = DataPageVersion,
        definition_levels = RevDefLevels,
        max_definition_level = MaxDefLevel
    } = C,
    serialize_levels(RevDefLevels, MaxDefLevel, DataPageVersion).

serialize_levels(RevLevels, MaxLevel, DataPageVersion) ->
    Levels = lists:reverse(RevLevels),
    BitWidth = bit_width_of(MaxLevel),
    Encoder0 = parquer_rle_bp_hybrid_encoder:new(BitWidth),
    Encoder1 = lists:foldl(
        fun(L, Acc) -> parquer_rle_bp_hybrid_encoder:write_int(Acc, L) end,
        Encoder0,
        Levels
    ),
    RLEBytes = parquer_rle_bp_hybrid_encoder:to_bytes(Encoder1),
    case DataPageVersion of
        1 ->
            [encode_data_size(iolist_size(RLEBytes)) | RLEBytes];
        2 ->
            RLEBytes
    end.

serialize_dictionary(#c{} = C, #writer{} = W) ->
    #c{data = #data{dictionary = Dictionary}} = C,
    {DictBin, ConcreteDict, _} = maps:fold(
        fun(V, _, {BinAcc0, DictAcc0, N0}) ->
            BinAcc = append_datum_bin(BinAcc0, V, C),
            DictAcc = DictAcc0#{V => N0},
            N = N0 + 1,
            {BinAcc, DictAcc, N}
        end,
        {<<>>, #{}, 0},
        Dictionary
    ),
    {DictBinComp, _} = compress_data(_RepLevelBin = [], _DefLevelBin = [], DictBin, C, W),
    NumDictKeys = map_size(ConcreteDict),
    DictBinSize = iolist_size(DictBin),
    DictBinCompSize = iolist_size(DictBinComp),
    #{
        ?num_dict_keys => NumDictKeys,
        ?concrete_dictionary => ConcreteDict,
        ?uncompressed => DictBin,
        ?compressed => DictBinComp,
        ?uncompressed_size => DictBinSize,
        ?compressed_size => DictBinCompSize
    }.

serialize_dictionary_page_header(SerializedInfo) ->
    #{
        ?num_dict_keys := NumDictKeys,
        ?uncompressed_size := DictBinSize,
        ?compressed_size := DictBinCompSize
    } = SerializedInfo,
    DictPageHeader = parquer_thrift_utils:dict_page_header(#{
        ?num_values => NumDictKeys,
        ?encoding => ?ENCODING_PLAIN_DICT,
        %% ?is_sorted => false
        ?is_sorted => ?undefined
    }),
    PageHeaderDict = parquer_thrift_utils:page_header(#{
        ?page_type => ?PAGE_TYPE_DICT_PAGE,
        ?uncompressed_page_size => DictBinSize,
        ?compressed_page_size => DictBinCompSize,
        ?dict_page_header => DictPageHeader
    }),
    parquer_thrift_utils:serialize(PageHeaderDict).

serialize_data_plain(#c{} = C, #writer{} = W) ->
    #c{data = #data{values = RevValues}} = C,
    RepLevelBin = serialize_repetition_levels(C),
    DefLevelBin = serialize_definition_levels(C),
    DataBin =
        lists:foldl(
            fun(Datum, Acc) ->
                append_datum_bin(Acc, Datum, C)
            end,
            <<>>,
            lists:reverse(RevValues)
        ),
    {ColDataBinComp, ColDataBin} = compress_data(RepLevelBin, DefLevelBin, DataBin, C, W),
    ColDataBinSize = iolist_size(ColDataBin),
    ColDataBinCompSize = iolist_size(ColDataBinComp),
    #{
        ?uncompressed => ColDataBin,
        ?compressed => ColDataBinComp,
        ?def_level_bin => DefLevelBin,
        ?rep_level_bin => RepLevelBin,
        ?uncompressed_size => ColDataBinSize,
        ?compressed_size => ColDataBinCompSize
    }.

serialize_data_with_dictionary(#c{} = C, #writer{} = W, ConcreteDict) ->
    #c{data = #data{values = RevValues}} = C,
    RepLevelBin = serialize_repetition_levels(C),
    DefLevelBin = serialize_definition_levels(C),
    DataBitWidth = bit_width_of(max(0, map_size(ConcreteDict) - 1)),
    DataEncoder0 = parquer_rle_bp_hybrid_encoder:new(DataBitWidth),
    DataEncoder1 =
        lists:foldl(
            fun(V, Acc) ->
                I = maps:get(V, ConcreteDict),
                parquer_rle_bp_hybrid_encoder:write_int(Acc, I)
            end,
            DataEncoder0,
            lists:reverse(RevValues)
        ),
    DataBin0 = parquer_rle_bp_hybrid_encoder:to_bytes(DataEncoder1),
    DataBin = [encode_bit_width(DataBitWidth) | DataBin0],
    {ColDataBinComp, ColDataBin} = compress_data(RepLevelBin, DefLevelBin, DataBin, C, W),
    ColDataBinSize = iolist_size(ColDataBin),
    ColDataBinCompSize = iolist_size(ColDataBinComp),
    #{
        ?uncompressed => ColDataBin,
        ?compressed => ColDataBinComp,
        ?def_level_bin => DefLevelBin,
        ?rep_level_bin => RepLevelBin,
        ?uncompressed_size => ColDataBinSize,
        ?compressed_size => ColDataBinCompSize
    }.

serialize_data_page(SerializedInfo, #c{} = C) ->
    case C#c.data_page_version of
        2 ->
            serialize_data_page_v2(SerializedInfo, #c{} = C);
        1 ->
            serialize_data_page_v1(SerializedInfo, #c{} = C)
    end.

serialize_data_page_v1(SerializedInfo, #c{} = C) ->
    #c{num_values = NumValues, primitive_type = PrimitiveType} = C,
    #{
        ?uncompressed_size := ColDataBinSize,
        ?compressed_size := ColDataBinCompSize
    } = SerializedInfo,
    IsDictEnabled = C#c.data#data.enable_dictionary,
    ValueEncoding =
        case IsDictEnabled of
            true -> ?ENCODING_PLAIN_DICT;
            false when PrimitiveType == ?BOOLEAN -> ?ENCODING_RLE;
            false -> ?ENCODING_PLAIN
        end,
    DataPageHeader = parquer_thrift_utils:data_page_header_v1(#{
        ?num_values => NumValues,
        ?encoding => ValueEncoding,
        ?definition_level_encoding => ?ENCODING_RLE,
        ?repetition_level_encoding => ?ENCODING_RLE
        %% TODO
        %% ?is_compressed => TODO
        %% ?statistics => TODO
    }),
    PageHeaderData = parquer_thrift_utils:page_header(#{
        ?page_type => ?PAGE_TYPE_DATA_PAGE_V1,
        ?uncompressed_page_size => ColDataBinSize,
        ?compressed_page_size => ColDataBinCompSize,
        ?data_page_header_v1 => DataPageHeader
    }),
    parquer_thrift_utils:serialize(PageHeaderData).

serialize_data_page_v2(SerializedInfo, #c{} = C) ->
    #c{
        num_values = NumValues,
        num_nulls = NumNulls,
        primitive_type = PrimitiveType
    } = C,
    #{
        ?uncompressed_size := ColDataBinSize,
        ?compressed_size := ColDataBinCompSize,
        ?def_level_bin := DefLevelBin,
        ?rep_level_bin := RepLevelBin
    } = SerializedInfo,
    IsDictEnabled = C#c.data#data.enable_dictionary,
    ValueEncoding =
        case IsDictEnabled of
            true -> ?ENCODING_PLAIN_DICT;
            false when PrimitiveType == ?BOOLEAN -> ?ENCODING_RLE;
            false -> ?ENCODING_PLAIN
        end,
    DataPageHeader = parquer_thrift_utils:data_page_header_v2(#{
        ?num_values => NumValues,
        ?num_nulls => NumNulls,
        ?num_rows => C#c.num_rows,
        ?encoding => ValueEncoding,
        ?definition_levels_byte_length => iolist_size(DefLevelBin),
        ?repetition_levels_byte_length => iolist_size(RepLevelBin)
        %% TODO
        %% ?is_compressed => TODO
        %% ?statistics => TODO
    }),
    PageHeaderData = parquer_thrift_utils:page_header(#{
        ?page_type => ?PAGE_TYPE_DATA_PAGE_V2,
        ?uncompressed_page_size => ColDataBinSize,
        ?compressed_page_size => ColDataBinCompSize,
        ?data_page_header_v2 => DataPageHeader
    }),
    parquer_thrift_utils:serialize(PageHeaderData).

%% TODO: add other types
append_datum_bin(BinAcc, Datum, #c{primitive_type = ?BYTE_ARRAY}) when is_binary(Datum) ->
    Size = encode_data_size(byte_size(Datum)),
    <<BinAcc/binary, Size/binary, Datum/binary>>;
append_datum_bin(BinAcc, Datum, #c{primitive_type = ?FIXED_LEN_BYTE_ARRAY, type_length = TypeLength}) when
    is_binary(Datum), is_integer(TypeLength)
->
    <<BinAcc/binary, Datum:TypeLength/binary>>;
append_datum_bin(BinAcc, Datum, #c{primitive_type = ?INT32}) when is_integer(Datum) ->
    <<BinAcc/binary, Datum:32/little-signed>>;
append_datum_bin(BinAcc, Datum, #c{primitive_type = ?INT64}) when is_integer(Datum) ->
    <<BinAcc/binary, Datum:64/little-signed>>;
append_datum_bin(BinAcc, Datum, #c{primitive_type = ?INT96}) when is_integer(Datum) ->
    <<BinAcc/binary, Datum:96/little-signed>>;
append_datum_bin(BinAcc, Datum, #c{primitive_type = ?FLOAT}) when is_number(Datum) ->
    <<BinAcc/binary, Datum:32/little-float>>;
append_datum_bin(BinAcc, Datum, #c{primitive_type = ?DOUBLE}) when is_number(Datum) ->
    <<BinAcc/binary, Datum:64/little-float>>.

bit_width_of(0) ->
    0;
bit_width_of(Level) ->
    floor(math:log2(Level) + 1).

encode_bit_width(BitWidth) ->
    <<BitWidth:8>>.

encode_data_size(Size) ->
    <<Size:32/little>>.

compress_data(RepLevelBin, DefLevelBin, DataBin, C, W) ->
    {Compression, Opts} = get_compression(C, W),
    case C#c.data_page_version of
        1 ->
            Uncompressed = [RepLevelBin, DefLevelBin, DataBin],
            Compressed = do_compress_data(Uncompressed, Compression, Opts),
            {Compressed, Uncompressed};
        2 ->
            Compressed = do_compress_data(DataBin, Compression, Opts),
            {[RepLevelBin, DefLevelBin, Compressed], [RepLevelBin, DefLevelBin, DataBin]}
    end.

get_compression(_Col, #writer{opts = Opts}) ->
    %% todo: per-column compression and compression opts
    CompressionOpts = #{},
    maps:get(?default_compression, Opts, {?DEFAULT_COMPRESSION, CompressionOpts}).

do_compress_data(DataBin, ?COMPRESSION_NONE, _Opts) ->
    DataBin;
do_compress_data(DataBin, ?COMPRESSION_ZSTD, _Opts) ->
    case ezstd:compress(iolist_to_binary(DataBin)) of
        {error, Reason} ->
            error({compression_failed, ?COMPRESSION_ZSTD, Reason, DataBin});
        Res when is_binary(Res) ->
            Res
    end;
do_compress_data(DataBin, ?COMPRESSION_SNAPPY, _Opts) ->
    case snappyer:compress(DataBin) of
        {ok, Res} ->
            Res;
        {error, Reason} ->
            error({compression_failed, ?COMPRESSION_SNAPPY, Reason, DataBin})
    end.

mk_column_chunk(WriteMeta, Col, Writer0) ->
    {PathInSchema, _} = lists:unzip(Col#c.path),
    {Compression, _} = get_compression(Col, Writer0),
    ColumnChunk = parquer_thrift_utils:column_chunk(#{
        ?offset => Writer0#writer.offset,
        ?metadata => parquer_thrift_utils:column_metadata(#{
            ?type => Col#c.primitive_type,
            ?num_values => Col#c.num_values,
            ?encodings => encodings_for(Col),
            ?path_in_schema => PathInSchema,
            ?codec => Compression,
            ?total_uncompressed_size => WriteMeta#write_meta.total_uncompressed_size,
            ?total_compressed_size => WriteMeta#write_meta.total_compressed_size,
            ?data_page_offset => data_page_offset(WriteMeta, Writer0),
            ?dict_page_offset => dict_page_offset(WriteMeta, Writer0)
        })
    }),
    Writer = Writer0#writer{
        offset = Writer0#writer.offset + WriteMeta#write_meta.total_compressed_size
    },
    {ColumnChunk, Writer}.

encodings_for(#c{primitive_type = ?BOOLEAN}) ->
    [?ENCODING_RLE];
encodings_for(#c{}) ->
    [?ENCODING_PLAIN, ?ENCODING_RLE, ?ENCODING_RLE_DICT].

dict_page_offset(#write_meta{dict_page_header_size = undefined}, _Writer) ->
    undefined;
dict_page_offset(#write_meta{}, #writer{offset = Offset}) ->
    %% Dict comes first, right at the current offset.
    Offset.

data_page_offset(#write_meta{dict_page_header_size = undefined}, #writer{offset = Offset}) ->
    %% Without dict, data comes right at the current offset.
    Offset;
data_page_offset(#write_meta{} = WriteMeta, #writer{offset = BaseOffset}) ->
    %% Dict comes first, then data.
    #write_meta{
        dict_page_header_size = DictHeaderSize,
        dict_compressed_size = DictSize
    } = WriteMeta,
    BaseOffset + DictHeaderSize + DictSize.

schema_to_thrift(#writer{schema = Schema}) ->
    lists:map(fun parquer_thrift_utils:schema_element/1, Schema).

is_dictionary_enabled(KeyPath, Opts) ->
    ColKeyPath = iolist_to_binary(lists:join($., KeyPath)),
    case Opts of
        #{?enable_dictionary := #{ColKeyPath := Bool}} ->
            Bool;
        #{?enable_dictionary := Bool} ->
            Bool
    end.

data_page_header_version(KeyPath, Opts) ->
    ColKeyPath = iolist_to_binary(lists:join($., KeyPath)),
    case Opts of
        #{?data_page_header_version := #{ColKeyPath := Vsn}} when ?IS_VALID_DATA_HEADER_VSN(Vsn) ->
            Vsn;
        #{?data_page_header_version := Vsn} when ?IS_VALID_DATA_HEADER_VSN(Vsn) ->
            Vsn;
        _ ->
            ?DEFAULT_DATA_HEADER_VSN
    end.

max_row_group_bytes(#writer{opts = #{?max_row_group_bytes := MaxRowGroupBytes}}) ->
    MaxRowGroupBytes.

write_metadata_column_id(#c{id = Id, name = Name}) ->
    #{?id => Id, ?name => Name}.
