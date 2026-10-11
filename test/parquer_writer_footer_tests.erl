%%--------------------------------------------------------------------
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
-module(parquer_writer_footer_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parquer_parquet_types.hrl").

%% `FileMetaData.num_rows' must be the number of rows in the FILE (sum of all row groups).
%% Readers that trust it (polars, `pyarrow.dataset().count_rows()', duckdb
%% `parquet_file_metadata()', catalog/manifest builders) otherwise see an empty file.

single_row_group_test() ->
    {RowGroups, NumRows} = footer(write_many(1000)),
    ?assertEqual([1000], RowGroups),
    ?assertEqual(1000, NumRows).

%% One record per row group: the last group is already emitted when `close/1' runs, which
%% exercises the `has_data = false' branch of `do_close/1'.
many_row_groups_test() ->
    {RowGroups, NumRows} = footer(write_one_by_one(100)),
    ?assertEqual(100, length(RowGroups)),
    ?assertEqual(100, lists:sum(RowGroups)),
    ?assertEqual(100, NumRows).

empty_file_test() ->
    {RowGroups, NumRows} = footer(write_many(0)),
    ?assertEqual(0, lists:sum(RowGroups)),
    ?assertEqual(0, NumRows).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

schema() ->
    parquer_schema:root(<<"root">>, [parquer_schema:int64(<<"f0">>, required, #{})]).

opts(MaxRowGroupBytes) ->
    #{
        default_compression => {none, #{}},
        %% the row group size estimate grows on the dictionary path
        enable_dictionary => true,
        max_row_group_bytes => MaxRowGroupBytes
    }.

records(N) ->
    [#{<<"f0">> => I} || I <- lists:seq(1, N)].

write_many(N) ->
    W0 = parquer_writer:new(schema(), opts(128 * 1024 * 1024)),
    {IO0, W1} = parquer_writer:write_many(W0, records(N)),
    {IO1, _} = parquer_writer:close(W1),
    iolist_to_binary([IO0, IO1]).

write_one_by_one(N) ->
    W0 = parquer_writer:new(schema(), opts(1)),
    {IO0, W1} = lists:foldl(
        fun(Rec, {Acc, W}) ->
            {IO, W2} = parquer_writer:write(W, Rec),
            {[Acc, IO], W2}
        end,
        {[], W0},
        records(N)
    ),
    {IO1, _} = parquer_writer:close(W1),
    iolist_to_binary([IO0, IO1]).

%% Decode the Thrift `FileMetaData' from the file tail:
%% ... | footer | footer length (4 bytes, little endian) | "PAR1"
footer(Bin) ->
    Size = byte_size(Bin),
    <<FooterLen:32/little, "PAR1">> = binary:part(Bin, Size - 8, 8),
    FooterBin = binary:part(Bin, Size - 8 - FooterLen, FooterLen),
    {ok, Transport} = thrift_membuffer_transport:new(FooterBin),
    {ok, Proto} = thrift_compact_protocol:new(Transport),
    {_, {ok, FMD}} = thrift_protocol:read(Proto, {struct, {parquer_parquet_types, 'fileMetaData'}}),
    #'fileMetaData'{num_rows = NumRows, row_groups = RowGroups} = FMD,
    {[RG#'rowGroup'.num_rows || RG <- RowGroups], NumRows}.
