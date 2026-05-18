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
-module(parquer_schema_avro).

%% API
-export([from_avro/1, from_avro/2]).

-include("parquer.hrl").

-moduledoc """
This modules handles converting Avro schemas to Parquet schema.
""".

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(t, <<"type">>).
-define(n, <<"name">>).
-define(i, <<"field-id">>).
-define(lt, <<"logicalType">>).

-define(write_old_list_structure, write_old_list_structure).

-define(IS_PRIMITIVE(T),
    ((T == <<"null">>) orelse
        (T == <<"string">>) orelse
        (T == <<"boolean">>) orelse
        (T == <<"int">>) orelse
        (T == <<"long">>) orelse
        (T == <<"float">>) orelse
        (T == <<"double">>) orelse
        (T == <<"bytes">>))
).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

from_avro(AvroSc) ->
    from_avro(AvroSc, _Opts = #{}).

from_avro(#{?t := <<"record">>} = AvroSc, Opts0) ->
    DefaultOpts = #{
        ?write_old_list_structure => false
    },
    Opts = maps:merge(DefaultOpts, Opts0),
    avro_record_to_parquet(AvroSc, ?REPETITION_REPEATED, Opts).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

avro_record_to_parquet(#{?t := <<"record">>} = AvroSc, Repetition, Opts) ->
    #{?n := Name, <<"fields">> := AvroFields} = AvroSc,
    ParquetFields = lists:map(fun(F) -> avro_field_to_parquet(F, Opts) end, AvroFields),
    parquer_schema:group(Name, Repetition, ParquetFields).

avro_field_to_parquet(AvroField, Opts) ->
    #{
        ?n := Name,
        ?t := AvroType0
    } = AvroField,
    {Repetition, AvroType} = parse_maybe_union(AvroType0),
    avro_schema_to_parquet(AvroType, Name, Repetition, AvroField, Opts).

avro_schema_to_parquet(<<"string">>, Name, Repetition, Parent, _Opts) ->
    %% fixme: uuid goes here too
    parquer_schema:string(Name, Repetition, common_opts(Parent));
avro_schema_to_parquet(<<"boolean">>, Name, Repetition, Parent, _Opts) ->
    parquer_schema:bool(Name, Repetition, common_opts(Parent));
avro_schema_to_parquet(<<"int">>, Name, Repetition, Parent, _Opts) ->
    parquer_schema:int32(Name, Repetition, common_opts(Parent));
avro_schema_to_parquet(<<"long">>, Name, Repetition, Parent, _Opts) ->
    parquer_schema:int64(Name, Repetition, common_opts(Parent));
avro_schema_to_parquet(<<"float">>, Name, Repetition, Parent, _Opts) ->
    parquer_schema:float(Name, Repetition, common_opts(Parent));
avro_schema_to_parquet(<<"double">>, Name, Repetition, Parent, _Opts) ->
    parquer_schema:double(Name, Repetition, common_opts(Parent));
avro_schema_to_parquet(<<"bytes">>, Name, Repetition, Parent, _Opts) ->
    %% TODO: add decimal logical type here...
    parquer_schema:byte_array(Name, Repetition, common_opts(Parent));
avro_schema_to_parquet(Types, Name, Repetition, Parent, Opts) when is_list(Types) ->
    TypeOpts0 = common_opts(Parent),
    TypeOpts = TypeOpts0#{
        ?logical_type => parquer_schema:lt_enum(),
        ?converted_type => ?CONVERTED_TYPE_ENUM
    },
    parquer_schema:group(
        Name,
        Repetition,
        TypeOpts,
        lists:map(
            fun({I, T}) ->
                Name1 = <<Name/binary, "_", (integer_to_binary(I))/binary>>,
                avro_schema_to_parquet(T, Name1, ?REPETITION_OPTIONAL, Parent, Opts)
            end,
            lists:enumerate(Types)
        )
    );
avro_schema_to_parquet(#{?t := <<"array">>} = Sc, Name, ListRepetition, Parent, Opts) ->
    #{<<"items">> := InnerSc0} = Sc,
    case Opts of
        #{?write_old_list_structure := false} ->
            {ElemRepetition, InnerSc1} = parse_maybe_union(InnerSc0),
            parquer_schema:list(Name, ListRepetition, common_opts(Parent), [
                parquer_schema:group(<<"list">>, ?REPETITION_REPEATED, [
                    avro_schema_to_parquet(InnerSc1, <<"element">>, ElemRepetition, Sc, Opts)
                ])
            ]);
        #{?write_old_list_structure := true} ->
            %% todo: raise a more descriptive error if the schema allows nulls here.
            %% currently, it just raises `{unsupported_type, null}`.
            maybe
                {?REPETITION_OPTIONAL, _} ?= parse_maybe_union(InnerSc0),
                %% Old schema does not support null elements.
                throw_unsupported_type(Sc, #{
                    hint => <<
                        "Null array elements are not allowed when "
                        "`write_old_list_structure=true`"
                    >>
                })
            end,
            parquer_schema:list(Name, ListRepetition, common_opts(Parent), [
                avro_schema_to_parquet(InnerSc0, <<"array">>, ?REPETITION_REPEATED, Sc, Opts)
            ])
    end;
avro_schema_to_parquet(#{?t := <<"map">>} = Sc, Name, MapRepetition, Parent, Opts) ->
    #{<<"values">> := ValuesSc0} = Sc,
    {ValuesRepetition, ValuesSc} = parse_maybe_union(ValuesSc0),
    ValueType = avro_schema_to_parquet(ValuesSc, <<"value">>, ValuesRepetition, Sc, Opts),
    parquer_schema:map(Name, MapRepetition, common_opts(Parent), ValueType);
avro_schema_to_parquet(#{?t := <<"record">>} = Sc, Name, Repetition, _Parent, Opts) ->
    avro_record_to_parquet(Sc#{?n => Name}, Repetition, Opts);
avro_schema_to_parquet(#{?t := <<"fixed">>} = Sc, Name, Repetition, Parent, _Opts) ->
    #{<<"size">> := TypeLength} = Sc,
    TypeOpts0 = common_opts(Parent),
    LogicalType = logical_type_of(Sc),
    TypeOpts = TypeOpts0#{
        ?type_length => TypeLength,
        ?logical_type => LogicalType
    },
    parquer_schema:fixed_len_byte_array(Name, Repetition, TypeOpts);
avro_schema_to_parquet(
    #{?t := <<"long">>, ?lt := <<"timestamp", _/binary>>} = Sc, Name, Repetition, Parent, _Opts
) ->
    TypeOpts0 = common_opts(Parent),
    LogicalType = logical_type_of(Sc),
    TypeOpts = TypeOpts0#{?logical_type => LogicalType},
    parquer_schema:int64(Name, Repetition, TypeOpts);
avro_schema_to_parquet(#{?t := <<"int">>, ?lt := <<"date">>} = Sc, Name, Repetition, Parent, _Opts) ->
    TypeOpts0 = common_opts(Parent),
    LogicalType = logical_type_of(Sc),
    TypeOpts = TypeOpts0#{?logical_type => LogicalType},
    parquer_schema:int32(Name, Repetition, TypeOpts);
avro_schema_to_parquet(
    #{?t := <<"long">>, ?lt := <<"time-", _/binary>>} = Sc, Name, Repetition, Parent, _Opts
) ->
    TypeOpts0 = common_opts(Parent),
    LogicalType = logical_type_of(Sc),
    TypeOpts = TypeOpts0#{?logical_type => LogicalType},
    parquer_schema:int64(Name, Repetition, TypeOpts);
avro_schema_to_parquet(Sc, _Name, _Repetition, _Parent, _Opts) ->
    throw_unsupported_type(Sc).

parse_maybe_union(Types) when is_list(Types) ->
    {MaybeNull, Rest} = lists:partition(fun(T) -> T == <<"null">> end, Types),
    Repetition =
        case MaybeNull of
            [] -> ?REPETITION_REQUIRED;
            [_ | _] -> ?REPETITION_OPTIONAL
        end,
    case Rest of
        [] ->
            %% todo: which type to use if always null?
            {Repetition, ?INT32};
        [T] ->
            {Repetition, T};
        _ ->
            {Repetition, Rest}
    end;
parse_maybe_union(TypeOrTypes) ->
    %% Primitive or complex type
    {?REPETITION_REQUIRED, TypeOrTypes}.

common_opts(#{?t := <<"array">>} = ParentSc) ->
    Id = maps:get(<<"element-id">>, ParentSc, ?undefined),
    #{?id => Id};
common_opts(ParentSc) ->
    Id = maps:get(?i, ParentSc, ?undefined),
    #{?id => Id}.

-spec throw_unsupported_type(binary() | list() | map()) -> no_return().
throw_unsupported_type(Sc) ->
    throw_unsupported_type(Sc, _ExtraContext = #{}).

-spec throw_unsupported_type(binary() | list() | map(), map()) -> no_return().
throw_unsupported_type(Sc, ExtraContext) ->
    throw(ExtraContext#{reason => unsupported_type, type => Sc}).

logical_type_of(#{<<"logicalType">> := <<"decimal">>} = Sc) ->
    Precision = maps:get(<<"precision">>, Sc),
    Scale = maps:get(<<"scale">>, Sc),
    parquer_schema:lt_decimal(#{?precision => Precision, ?scale => Scale});
logical_type_of(#{<<"logicalType">> := <<"timestamp-micros">>, <<"adjust-to-utc">> := AdjustToUTC}) ->
    parquer_schema:lt_timestamp(#{?is_adjusted_to_utc => AdjustToUTC, ?unit => ?time_unit_micros});
logical_type_of(#{<<"logicalType">> := <<"timestamp-nanos">>, <<"adjust-to-utc">> := AdjustToUTC}) ->
    parquer_schema:lt_timestamp(#{?is_adjusted_to_utc => AdjustToUTC, ?unit => ?time_unit_nanos});
logical_type_of(#{<<"logicalType">> := <<"date">>}) ->
    parquer_schema:lt_date();
logical_type_of(#{<<"logicalType">> := <<"time-micros">>} = Sc) ->
    AdjustToUTC = maps:get(<<"adjust-to-utc">>, Sc, false),
    parquer_schema:lt_time(#{?is_adjusted_to_utc => AdjustToUTC, ?unit => ?time_unit_micros});
logical_type_of(_) ->
    ?undefined.
