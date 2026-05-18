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
-module(parquer_writer_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("parquer.hrl").
-include("parquer_parquet_types.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(F0, <<"f0">>).
-define(F1, <<"f1">>).
-define(F2, <<"f2">>).

-define(data_page_header_version, data_page_header_version).
-define(data_page_v1, data_page_v1).
-define(data_page_v2, data_page_v2).
-define(dict_disabled, dict_disabled).
-define(dict_enabled, dict_enabled).
-define(enable_dictionary, enable_dictionary).
-define(max_row_group_bytes, max_row_group_bytes).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    parquer_test_utils:all(?MODULE).

groups() ->
    parquer_test_utils:groups(?MODULE).

init_per_suite(Config) ->
    Dir = code:lib_dir(parquer),
    CD = filename:join([Dir, "test", "clj"]),
    Opts = #{
        program => os:find_executable("clojure"),
        args => ["-M", "-m", "writer-oracle"],
        cd => CD
    },
    ct:print("starting oracle"),
    {ok, Oracle} = parquer_oracle:start(Opts),
    ct:print("oracle started"),
    [{oracle, Oracle} | Config].

end_per_suite(Config) ->
    Oracle = ?config(oracle, Config),
    is_process_alive(Oracle) andalso parquer_oracle:stop(Oracle),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    link(?config(oracle, TCConfig)),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true ->
            ?MODULE:TestCase(init, TCConfig);
        false ->
            TCConfig
    end.

end_per_testcase(TestCase, TCConfig) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 2),
        ?MODULE:TestCase('end', TCConfig)
    end,
    unlink(?config(oracle, TCConfig)),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

query_oracle(Parquet, TCConfig) ->
    Oracle = ?config(oracle, TCConfig),
    Payload = [base64:encode(iolist_to_binary(Parquet)), $\n],
    ct:pal("querying oracle"),
    {ok, Data0} = parquer_oracle:command(Oracle, Payload),
    Data1 = iolist_to_binary(Data0),
    Data = json:decode(Data1),
    ct:pal("oracle responded:\n  ~p", [Data]),
    Data.

single_field_schema(Repetition, Type) when is_atom(Type) ->
    single_field_schema(Repetition, Type, _Opts = #{}).
single_field_schema(Repetition, Type, Opts) when is_atom(Type) ->
    parquer_schema:root(<<"root">>, [parquer_schema:Type(?F0, Repetition, Opts)]).

write_and_close(Writer0, Records) ->
    {IOData0, Writer1} = parquer_writer:write_many(Writer0, Records),
    {IOData1, _WriteMetadata} = parquer_writer:close(Writer1),
    [IOData0, IOData1].

opts_of(TCConfig) ->
    lists:foldl(
        fun
            (?dict_enabled, Acc) ->
                Acc#{?enable_dictionary => true};
            (?dict_disabled, Acc) ->
                Acc#{?enable_dictionary => false};
            (?data_page_v1, Acc) ->
                Acc#{?data_page_header_version => 1};
            (?data_page_v2, Acc) ->
                Acc#{?data_page_header_version => 2};
            (compression_none, Acc) ->
                Acc#{?default_compression => {?COMPRESSION_NONE, #{}}};
            (compression_zstd, Acc) ->
                Acc#{?default_compression => {?COMPRESSION_ZSTD, #{}}};
            (compression_snappy, Acc) ->
                Acc#{?default_compression => {?COMPRESSION_SNAPPY, #{}}};
            (_, Acc) ->
                Acc
        end,
        #{},
        parquer_test_utils:group_path(TCConfig)
    ).

%% N.B.: Java implementation does not like a repeated field that is not a group (at least
%% of string type).
%%
%% e.g.:
%%   Execution error (ClassCastException) at org.apache.parquet.schema.Type/asGroupType
%%   (Type.java:247).  repeated binary f is not a group
%%
%% Also, it does not like nulls in repeated primitive fields (unless
%% `parquet.avro.write-old-list-structure=false`, in which case the schema
%% representation is different).
%%
%% This attempts to mimick the following Parquet schema:
%%    message root {
%%      repeated group f0 {
%%        optional binary f1;
%%      }
%%    }
%% ... which is produced by `MessageTypeParser/parseMessageType` followed by
%% `AvroSchemaConverter/convert`
smoke_repeated_schema1(Type) ->
    smoke_repeated_schema1(Type, _Opts = #{}).
smoke_repeated_schema1(Type, Opts) ->
    parquer_schema:root(
        <<"root">>,
        [
            parquer_schema:group(
                ?F0,
                ?REPETITION_REQUIRED,
                #{?converted_type => ?CONVERTED_TYPE_LIST},
                [
                    parquer_schema:group(
                        <<"array">>,
                        ?REPETITION_REPEATED,
                        [parquer_schema:Type(?F1, ?REPETITION_OPTIONAL, Opts)]
                    )
                ]
            )
        ]
    ).

smoke_schema1(?REPETITION_REPEATED, Type, Opts) ->
    smoke_repeated_schema1(Type, Opts);
smoke_schema1(Repetition, Type, Opts) ->
    single_field_schema(Repetition, Type, Opts).

%% Apparently, `AvroParquetReader` with `parquet.avro.readInt96AsFixed=true` reads this
%% deprecated type like this.
reader_int96(I) ->
    Bin = <<I:96/signed-little>>,
    Bytes = binary_to_list(Bin),
    lists:map(
        fun(U) ->
            <<S:8/signed>> = <<U:8/unsigned>>,
            S
        end,
        Bytes
    ).

smoke_values1(A) when is_atom(A) ->
    smoke_values1(atom_to_list(A));
smoke_values1("int32") ->
    #{values => [-1, 0, 2147483647]};
smoke_values1("int64") ->
    #{values => [-1, 0, 9223372036854775807]};
smoke_values1("int96") ->
    Values = [-1, 0, 39614081257132168796771975167],
    RoundtripValues = lists:map(fun reader_int96/1, Values),
    #{
        values => Values,
        roundtrip_values => RoundtripValues
    };
smoke_values1("float") ->
    <<V0:32/float>> = <<16#7f, 16#7f, 16#ff, 16#ff>>,
    V1 = -V0,
    %% Cannot represent +- inf nor nans in erlang.
    V2 = 0.0,
    Values = [V0, V1, V2],
    #{
        values => Values,
        roundtrip_values => [3.4028235e38, -3.4028235e38, 0.0]
    };
smoke_values1("double") ->
    <<V0:64/float>> = <<16#7f, 16#ef, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff>>,
    V1 = -V0,
    %% Cannot represent +- inf nor nans in erlang.
    V2 = 0.0,
    Values = [V0, V1, V2],
    #{
        values => Values,
        roundtrip_values => [1.7976931348623157e308, -1.7976931348623157e308, 0.0]
    };
smoke_values1("fixed_len_byte_array" ++ _) ->
    %% Values come back as byte arrays
    #{
        values => [<<"abc">>, <<"def">>, <<"ghi">>],
        type => fixed_len_byte_array,
        roundtrip_values => ["abc", "def", "ghi"]
    }.

%% Type matrix for `t_smoke_types_*` tests.
types1() ->
    [int32, int64, int96, float, double, {fixed_len_byte_array, #{type_length => 3}}].

compressions() ->
    [compression_none, compression_zstd, compression_snappy].

get_matrix_type_opts(TypeGroup) ->
    parquer_test_utils:get_matrix_params(?MODULE, TypeGroup, #{}).

group_path(TCConfig, Default) ->
    case parquer_test_utils:group_path(TCConfig) of
        [] ->
            Default;
        Path ->
            Path
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_smoke_binary_optional() ->
    [{matrix, true}].
t_smoke_binary_optional(matrix) ->
    [
        [Dict, DataPage]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_smoke_binary_optional(TCConfig) when is_list(TCConfig) ->
    Schema = single_field_schema(?REPETITION_OPTIONAL, string),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => <<"hello">>},
        #{},
        #{?F0 => <<"world">>}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := <<"hello">>},
            #{?F0 := null},
            #{?F0 := <<"world">>}
        ],
        Reference
    ),
    ok.

t_smoke_binary_required() ->
    [{matrix, true}].
t_smoke_binary_required(matrix) ->
    [
        [Dict, DataPage]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_smoke_binary_required(TCConfig) when is_list(TCConfig) ->
    Schema = single_field_schema(?REPETITION_REQUIRED, string),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => <<"hello">>},
        #{?F0 => <<"world">>}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := <<"hello">>},
            #{?F0 := <<"world">>}
        ],
        Reference
    ),
    ?assertError(
        #{reason := missing_required_value},
        write_and_close(Writer0, [#{}])
    ),
    ok.

t_smoke_binary_repeated() ->
    [{matrix, true}].
t_smoke_binary_repeated(matrix) ->
    [
        [Dict, DataPage]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_smoke_binary_repeated(TCConfig) when is_list(TCConfig) ->
    Schema = smoke_repeated_schema1(byte_array),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => [#{?F1 => <<"hello">>}, #{?F1 => <<"world">>}]},
        #{?F0 => []},
        #{?F0 => ?undefined},
        #{?F0 => ?null},
        #{?F0 => [#{?F1 => <<"!">>}, #{}]}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := [#{?F1 := <<"hello">>}, #{?F1 := <<"world">>}]},
            #{?F0 := []},
            #{?F0 := []},
            #{?F0 := []},
            #{?F0 := [#{?F1 := <<"!">>}, #{?F1 := null}]}
        ],
        Reference
    ),
    ok.

t_smoke_boolean_optional() ->
    [{matrix, true}].
t_smoke_boolean_optional(matrix) ->
    [
        [Dict, DataPage]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_smoke_boolean_optional(TCConfig) when is_list(TCConfig) ->
    Schema = single_field_schema(?REPETITION_OPTIONAL, bool),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => false},
        #{},
        ?null,
        #{?F0 => ?undefined},
        #{?F0 => ?null},
        #{?F0 => true}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := false},
            #{?F0 := null},
            #{?F0 := null},
            #{?F0 := null},
            #{?F0 := null},
            #{?F0 := true}
        ],
        Reference
    ),
    ok.

t_smoke_boolean_required() ->
    [{matrix, true}].
t_smoke_boolean_required(matrix) ->
    [
        [Dict, DataPage]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_smoke_boolean_required(TCConfig) when is_list(TCConfig) ->
    Schema = single_field_schema(?REPETITION_REQUIRED, bool),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => false},
        #{?F0 => true}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := false},
            #{?F0 := true}
        ],
        Reference
    ),
    ok.

t_smoke_boolean_repeated() ->
    [{matrix, true}].
t_smoke_boolean_repeated(matrix) ->
    [
        [Dict, DataPage]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_smoke_boolean_repeated(TCConfig) when is_list(TCConfig) ->
    Schema = smoke_repeated_schema1(bool),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => [#{?F1 => false}, #{?F1 => true}]},
        #{?F0 => []},
        #{?F0 => ?undefined},
        #{?F0 => [#{?F1 => true}, #{}]}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := [#{?F1 := false}, #{?F1 := true}]},
            #{?F0 := []},
            #{?F0 := []},
            #{?F0 := [#{?F1 := true}, #{?F1 := null}]}
        ],
        Reference
    ),
    ok.

t_smoke_types_required() ->
    [{matrix, true}].
t_smoke_types_required(matrix) ->
    [
        [Dict, DataPage, Type]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2],
        Type <- types1()
    ];
t_smoke_types_required(TCConfig) when is_list(TCConfig) ->
    [_, _, TypeGroup] = parquer_test_utils:group_path(TCConfig),
    #{values := [V0, V1 | _] = Vs} =
        SVs =
        smoke_values1(TypeGroup),
    Type = maps:get(type, SVs, TypeGroup),
    [EV0, EV1 | _] = EVs = maps:get(roundtrip_values, SVs, Vs),
    TypeOpts = get_matrix_type_opts(TypeGroup),
    Schema = single_field_schema(?REPETITION_REQUIRED, Type, TypeOpts),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => V0},
        #{?F0 => V1}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := EV0},
            #{?F0 := EV1}
        ],
        Reference,
        #{
            expected => EVs,
            inputs__ => Vs
        }
    ),
    ok.

t_smoke_types_optional() ->
    [{matrix, true}].
t_smoke_types_optional(matrix) ->
    [
        [Dict, DataPage, Type]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2],
        Type <- types1()
    ];
t_smoke_types_optional(TCConfig) when is_list(TCConfig) ->
    [_, _, TypeGroup] = parquer_test_utils:group_path(TCConfig),
    #{values := [V0, V1 | _] = Vs} =
        SVs =
        smoke_values1(TypeGroup),
    Type = maps:get(type, SVs, TypeGroup),
    [EV0, EV1 | _] = EVs = maps:get(roundtrip_values, SVs, Vs),
    TypeOpts = get_matrix_type_opts(TypeGroup),
    Schema = single_field_schema(?REPETITION_OPTIONAL, Type, TypeOpts),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => V0},
        #{},
        #{?F0 => V1}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertMatch(
        [
            #{?F0 := EV0},
            #{?F0 := null},
            #{?F0 := EV1}
        ],
        Reference,
        #{
            expected => EVs,
            inputs__ => Vs
        }
    ),
    ok.

t_smoke_types_repeated() ->
    [{matrix, true}].
t_smoke_types_repeated(matrix) ->
    [
        [Dict, DataPage, Type]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2],
        Type <- types1()
    ];
t_smoke_types_repeated(TCConfig) when is_list(TCConfig) ->
    [_, _, TypeGroup] = parquer_test_utils:group_path(TCConfig),
    #{values := [V0, V1, V2 | _] = Vs} =
        SVs =
        smoke_values1(TypeGroup),
    Type = maps:get(type, SVs, TypeGroup),
    [EV0, EV1, EV2 | _] = EVs = maps:get(roundtrip_values, SVs, Vs),
    TypeOpts = get_matrix_type_opts(TypeGroup),
    Schema = smoke_repeated_schema1(Type, TypeOpts),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => [#{?F1 => V0}, #{?F1 => V1}]},
        #{?F0 => []},
        #{?F0 => ?undefined},
        #{?F0 => ?null},
        #{?F0 => [#{?F1 => V2}, #{}]}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    %% OTP bug?!? CT bug?!?
    %% If this is an `?assertMatch` with EV0, ..., EV2, for int64 this fails, and the
    %% printed values all match......
    ?assertEqual(
        [
            #{?F0 => [#{?F1 => EV0}, #{?F1 => EV1}]},
            #{?F0 => []},
            #{?F0 => []},
            #{?F0 => []},
            #{?F0 => [#{?F1 => EV2}, #{?F1 => null}]}
        ],
        Reference,
        #{
            expected => EVs,
            inputs__ => Vs
        }
    ),
    ok.

t_multiple_row_groups_1_column() ->
    [{matrix, true}].
t_multiple_row_groups_1_column(matrix) ->
    [
        [Repetition, Type, Dict, DataPage]
     || Repetition <- [?REPETITION_OPTIONAL, ?REPETITION_REQUIRED, ?REPETITION_REPEATED],
        Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2],
        Type <- types1()
    ];
t_multiple_row_groups_1_column(TCConfig) when is_list(TCConfig) ->
    [Repetition, TypeGroup | _] =
        group_path(
            TCConfig,
            [?REPETITION_REQUIRED, int32, ?dict_disabled, ?data_page_v1]
        ),
    #{values := [V0, V1, V2 | _] = Vs} =
        SVs =
        smoke_values1(TypeGroup),
    Type = maps:get(type, SVs, TypeGroup),
    [EV0, EV1, EV2 | _] = EVs = maps:get(roundtrip_values, SVs, Vs),
    TypeOpts = get_matrix_type_opts(TypeGroup),
    Schema = smoke_schema1(Repetition, Type, TypeOpts),
    Opts0 = opts_of(TCConfig),
    Opts = Opts0#{?max_row_group_bytes => 1},
    Writer0 = parquer_writer:new(Schema, Opts),
    %% Each record shall go into an individual row group
    Records =
        lists:map(
            fun
                (V) when Repetition == ?REPETITION_REPEATED ->
                    #{?F0 => [#{?F1 => V}]};
                (V) ->
                    #{?F0 => V}
            end,
            [V0, V1, V2]
        ),
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    case Repetition of
        ?REPETITION_REPEATED ->
            ?assertEqual(
                [
                    #{?F0 => [#{?F1 => EV0}]},
                    #{?F0 => [#{?F1 => EV1}]},
                    #{?F0 => [#{?F1 => EV2}]}
                ],
                Reference,
                #{
                    expected => EVs,
                    inputs__ => Vs
                }
            );
        _ ->
            ?assertEqual(
                [
                    #{?F0 => EV0},
                    #{?F0 => EV1},
                    #{?F0 => EV2}
                ],
                Reference,
                #{
                    expected => EVs,
                    inputs__ => Vs
                }
            )
    end,
    ok.

t_multiple_columns() ->
    [{matrix, true}].
t_multiple_columns(matrix) ->
    [
        [Dict, DataPage]
     || Dict <- [?dict_enabled, ?dict_disabled],
        DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_multiple_columns(TCConfig) when is_list(TCConfig) ->
    Schema =
        parquer_schema:root(
            <<"root">>,
            [
                parquer_schema:list(
                    ?F0,
                    ?REPETITION_REQUIRED,
                    [parquer_schema:int32(<<"array">>, ?REPETITION_REPEATED)]
                ),
                parquer_schema:string(?F1, ?REPETITION_OPTIONAL),
                parquer_schema:bool(?F2, ?REPETITION_REQUIRED)
            ]
        ),
    Opts0 = opts_of(TCConfig),
    Opts = Opts0#{?max_row_group_bytes => 1},
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => [0, 1], ?F1 => <<"hi">>, ?F2 => true},
        #{?F0 => [], ?F1 => ?undefined, ?F2 => false},
        #{?F0 => [2], ?F1 => <<"world">>, ?F2 => false}
    ],
    Parquet = write_and_close(Writer0, Records),
    Reference = query_oracle(Parquet, TCConfig),
    ?assertEqual(
        [
            #{?F0 => [0, 1], ?F1 => <<"hi">>, ?F2 => true},
            #{?F0 => [], ?F1 => null, ?F2 => false},
            #{?F0 => [2], ?F1 => <<"world">>, ?F2 => false}
        ],
        Reference
    ),
    ok.

t_duplicate_values_with_dict() ->
    [{matrix, true}].
t_duplicate_values_with_dict(matrix) ->
    [
        [DataPage]
     || DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_duplicate_values_with_dict(TCConfig) when is_list(TCConfig) ->
    Schema = smoke_schema1(?REPETITION_REQUIRED, string, _TypeOpts = #{}),
    Opts0 = opts_of(TCConfig),
    Opts = Opts0#{?enable_dictionary => true},
    Writer0 = parquer_writer:new(Schema, Opts),
    Records = [
        #{?F0 => <<"hi">>},
        #{?F0 => <<"hi">>},
        #{?F0 => <<"world">>},
        #{?F0 => <<"hi">>}
    ],
    {IO0, Writer1} = parquer_writer:write_many(Writer0, Records),
    {IO1, _} = parquer_writer:close(Writer1),
    Reference = query_oracle([IO0, IO1], TCConfig),
    ?assertEqual(
        [
            #{?F0 => <<"hi">>},
            #{?F0 => <<"hi">>},
            #{?F0 => <<"world">>},
            #{?F0 => <<"hi">>}
        ],
        Reference
    ),
    ok.

t_compressions() ->
    [{matrix, true}].
t_compressions(matrix) ->
    [[Compression] || Compression <- compressions()];
t_compressions(TCConfig) when is_list(TCConfig) ->
    Schema = smoke_schema1(?REPETITION_REQUIRED, string, _TypeOpts = #{}),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Record = #{?F0 => <<"hiiiiiii">>},
    {IO0, Writer1} = parquer_writer:write(Writer0, Record),
    {IO1, _} = parquer_writer:close(Writer1),
    Reference = query_oracle([IO0, IO1], TCConfig),
    ?assertEqual([#{?F0 => <<"hiiiiiii">>}], Reference),
    ok.

t_dict_overflow() ->
    [{matrix, true}].
t_dict_overflow(matrix) ->
    [
        [DataPage]
     || DataPage <- [?data_page_v1, ?data_page_v2]
    ];
t_dict_overflow(TCConfig) when is_list(TCConfig) ->
    Schema = smoke_schema1(?REPETITION_REQUIRED, int64, _TypeOpts = #{}),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    %% See `parquer_writer:?MAX_DICT_SIZE`.
    NumRecords = 101,
    try
        persistent_term:put({parquet_writer, max_dict_size}, NumRecords - 1),
        Records = lists:map(fun(N) -> #{?F0 => N} end, lists:seq(1, NumRecords)),
        {IO0, Writer1} = parquer_writer:write_many(Writer0, Records),
        {IO1, _} = parquer_writer:close(Writer1),
        %% Should be readable by oracle.
        Reference = query_oracle([IO0, IO1], TCConfig),
        ?assertEqual(Records, Reference),
        ok
    after
        persistent_term:erase({parquer_writer, max_dict_size}),
        ok
    end,
    ok.

t_empty_column() ->
    [{matrix, true}].
t_empty_column(matrix) ->
    [
        [DataPage, Compression]
     || DataPage <- [?data_page_v1, ?data_page_v2],
        Compression <- compressions()
    ];
t_empty_column(TCConfig) when is_list(TCConfig) ->
    AvroSchemaRaw = ~b"""
       {
        "name": "MessageRecord",
        "type": "record",
        "fields": [
          {
            "name": "time",
            "type": "long"
          },
          {
            "name": "metric",
            "type": "string"
          }
        ]
      }
    """,
    AvroSchema = json:decode(AvroSchemaRaw),
    Schema = parquer_schema_avro:from_avro(AvroSchema),
    Opts = opts_of(TCConfig),
    Writer0 = parquer_writer:new(Schema, Opts),
    Record = #{<<"metric">> => <<"mymetric">>, <<"time">> => ?undefined},
    ?assertError(
        #{
            reason := missing_required_value,
            path := [{<<"time">>, ?REPETITION_REQUIRED}],
            value := #{<<"metric">> := _, <<"time">> := ?undefined}
        },
        parquer_writer:write(Writer0, Record)
    ),
    ok.
