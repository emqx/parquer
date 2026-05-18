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
-module(parquer_zipper_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parquer.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

flatten_1_test_() ->
    [
        {"nested lists 1",
            ?_assertEqual(
                [
                    {0, 2, 1},
                    {2, 2, 2},
                    {2, 2, 3},
                    {1, 2, 4},
                    {2, 2, 5},
                    {1, 1, ?undefined},
                    {1, 2, 6}
                ],
                parquer_zipper:flatten(
                    [
                        {<<"a">>, ?REPETITION_REPEATED},
                        {<<"b">>, ?REPETITION_REPEATED}
                    ],
                    [[1, 2, 3], [4, 5], [], [6]]
                )
            )},
        {"nested lists 2",
            ?_assertEqual(
                [
                    {0, 1, ?undefined},
                    {1, 2, 1},
                    {2, 2, 2},
                    {2, 2, 3},
                    {1, 2, 4},
                    {2, 2, 5},
                    {1, 1, ?undefined},
                    {1, 2, 6}
                ],
                parquer_zipper:flatten(
                    [
                        {<<"a">>, ?REPETITION_REPEATED},
                        {<<"b">>, ?REPETITION_REPEATED}
                    ],
                    [[], [1, 2, 3], [4, 5], [], [6]]
                )
            )}
    ].

blog_example_test_() ->
    %% https://blog.x.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
    %% message AddressBook {
    %%   required string owner;
    %%   repeated string ownerPhoneNumbers;
    %%   repeated group contacts {
    %%     required string name;
    %%     optional string phoneNumber;
    %%   }
    %% }
    OwnerCol = [{<<"owner">>, ?REPETITION_REQUIRED}],
    OwnerPhoneCol = [{<<"ownerPhoneNumbers">>, ?REPETITION_REPEATED}],
    ContactNameCol = [
        {<<"contacts">>, ?REPETITION_REPEATED},
        {<<"name">>, ?REPETITION_REQUIRED}
    ],
    ContactPhoneCol = [
        {<<"contacts">>, ?REPETITION_REPEATED},
        {<<"phoneNumber">>, ?REPETITION_OPTIONAL}
    ],
    Rec1 = #{
        <<"owner">> => <<"Julien Le Dem">>,
        <<"ownerPhoneNumbers">> => [
            <<"555 123 4567">>,
            <<"555 666 1337">>
        ],
        <<"contacts">> => [
            #{
                <<"name">> => <<"Dmitriy Ryaboy">>,
                <<"phoneNumber">> => <<"555 987 6543">>
            },
            #{<<"name">> => <<"Chris Aniszczyk">>}
        ]
    },
    Rec2 = #{
        <<"owner">> => <<"A. Nonymous">>
    },
    [
        {"owner 1",
            ?_assertEqual(
                [{0, 0, <<"Julien Le Dem">>}],
                parquer_zipper:flatten(OwnerCol, Rec1)
            )},
        {"ownerPhoneNumbers 1",
            ?_assertEqual(
                [
                    {0, 1, <<"555 123 4567">>},
                    {1, 1, <<"555 666 1337">>}
                ],
                parquer_zipper:flatten(OwnerPhoneCol, Rec1)
            )},
        {"ownerPhoneNumbers 2",
            ?_assertEqual(
                [{0, 0, ?undefined}],
                parquer_zipper:flatten(OwnerPhoneCol, Rec2)
            )},
        {"ownerPhoneNumbers 1",
            ?_assertEqual(
                [
                    {0, 1, <<"555 123 4567">>},
                    {1, 1, <<"555 666 1337">>}
                ],
                parquer_zipper:flatten(OwnerPhoneCol, Rec1)
            )},
        {"contact.name 1",
            ?_assertEqual(
                [
                    {0, 1, <<"Dmitriy Ryaboy">>},
                    {1, 1, <<"Chris Aniszczyk">>}
                ],
                parquer_zipper:flatten(ContactNameCol, Rec1)
            )},
        {"contact.name 2",
            ?_assertEqual(
                [{0, 0, ?undefined}],
                parquer_zipper:flatten(ContactNameCol, Rec2)
            )},
        {"contact.phoneNumber 1",
            ?_assertEqual(
                [
                    {0, 2, <<"555 987 6543">>},
                    {1, 1, ?undefined}
                ],
                parquer_zipper:flatten(ContactPhoneCol, Rec1)
            )},
        {"contact.phoneNumber 2",
            ?_assertEqual(
                [{0, 0, ?undefined}],
                parquer_zipper:flatten(ContactPhoneCol, Rec2)
            )}
    ].

%%%% Invalid schema
%%%% Invalid list type repeated group list (LIST) { required binary element (STRING); }
%% flatten_3_test_() ->
%%   %% message root {
%%   %%   optional group nest {
%%   %%     optional group thing (LIST) {
%%   %%       repeated group list {
%%   %%         required binary element (STRING);
%%   %%       }
%%   %%     }
%%   %%   }
%%   %% }
%%   %% Max definition level: 4
%%   %% Max repetition level: 1
%%   Col = [
%%     {<<"nest">>, ?REPETITION_OPTIONAL},
%%     {<<"thing">>, ?REPETITION_OPTIONAL},
%%     {<<"list">>, ?REPETITION_REPEATED},
%%     {<<"element">>, ?REPETITION_REQUIRED}
%%   ],
%%   [ ?_assertEqual(
%%       [ {0, 4, <<"hi">>}
%%       , {1, 4, <<"world">>}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{<<"nest">> => #{<<"thing">> => [<<"hi">>, <<"world">>]}}))
%%   , ?_assertEqual(
%%       [ {0, 2, ?undefined}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{<<"nest">> => #{<<"thing">> => []}}))
%%   , ?_assertEqual(
%%       [ {0, 3, ?undefined}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{<<"nest">> => #{<<"thing">> => [?undefined]}}))
%%   , ?_assertEqual(
%%       [ {0, 2, ?undefined}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{<<"nest">> => #{<<"thing">> => []}}))
%%   , ?_assertEqual(
%%       [ {0, 2, ?undefined}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{<<"nest">> => #{<<"thing">> => ?undefined}}))
%%   , ?_assertEqual(
%%       [ {0, 1, ?undefined}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{<<"nest">> => #{}}))
%%   , ?_assertEqual(
%%       [ {0, 1, ?undefined}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{<<"nest">> => ?undefined}))
%%   , ?_assertEqual(
%%       [ {0, 0, ?undefined}
%%       ],
%%       parquer_zipper:flatten(
%%         Col,
%%         #{}))
%%   ].

flatten_4_test_() ->
    %% message root {
    %%   optional group nest {
    %%     optional group thing (LIST) {
    %%       repeated group list (LIST) {
    %%         repeated binary element (STRING);
    %%       }
    %%     }
    %%   }
    %% }
    %% Max definition level: 4
    %% Max repetition level: 2
    Col = [
        {<<"nest">>, ?REPETITION_OPTIONAL},
        {<<"thing">>, ?REPETITION_OPTIONAL},
        {<<"list">>, ?REPETITION_REPEATED},
        {<<"element">>, ?REPETITION_REPEATED}
    ],
    [
        ?_assertEqual(
            [
                {0, 4, <<"hi">>},
                {2, 4, <<"world">>}
            ],
            parquer_zipper:flatten(
                Col,
                #{<<"nest">> => #{<<"thing">> => [[<<"hi">>, <<"world">>]]}}
            )
        ),
        ?_assertEqual(
            [{0, 1, ?undefined}],
            parquer_zipper:flatten(
                Col,
                #{<<"nest">> => #{}}
            )
        ),
        ?_assertEqual(
            [{0, 2, ?undefined}],
            parquer_zipper:flatten(
                Col,
                #{<<"nest">> => #{<<"thing">> => []}}
            )
        )
    ].

%% Based off output from Java reference implementation
flatten_5_test_() ->
    F = <<"x">>,
    Col = [{F, ?REPETITION_REPEATED}],
    [
        ?_assertEqual(
            [{0, 0, ?undefined}],
            parquer_zipper:flatten(
                Col,
                #{F => []}
            )
        ),
        ?_assertEqual(
            [
                {0, 1, <<"hi">>},
                {1, 1, <<"world">>}
            ],
            parquer_zipper:flatten(
                Col,
                #{F => [<<"hi">>, <<"world">>]}
            )
        )
    ].

%% Based off output from Java reference implementation.
list_1_test_() ->
    %% message root {
    %%   required group f0 (LIST) {
    %%     repeated group array {
    %%       optional binary f1;
    %%     }
    %%   }
    %% }
    F0 = <<"f0">>,
    F1 = <<"f1">>,
    Col = [
        {F0, ?REPETITION_REQUIRED},
        {<<"array">>, ?REPETITION_REPEATED},
        {F1, ?REPETITION_OPTIONAL}
    ],
    [
        ?_assertEqual(
            [{0, 0, ?undefined}],
            parquer_zipper:flatten(
                Col,
                #{F0 => []}
            )
        ),
        ?_assertEqual(
            [
                {0, 2, <<"hi">>},
                {1, 1, ?undefined}
            ],
            parquer_zipper:flatten(
                Col,
                #{
                    F0 => [
                        #{F1 => <<"hi">>},
                        #{}
                    ]
                }
            )
        ),
        ?_assertEqual(
            [
                {0, 2, <<"hi">>},
                {1, 1, ?undefined},
                {1, 1, ?undefined}
            ],
            parquer_zipper:flatten(
                Col,
                #{
                    F0 => [
                        #{F1 => <<"hi">>},
                        #{F1 => ?undefined},
                        #{}
                    ]
                }
            )
        ),
        ?_assertEqual(
            [
                {0, 2, <<"hi">>},
                {1, 1, ?undefined},
                {1, 1, ?undefined}
            ],
            parquer_zipper:flatten(
                Col,
                #{
                    F0 => [
                        #{F1 => <<"hi">>},
                        #{F1 => ?null},
                        #{}
                    ]
                }
            )
        )
    ].

next_at_end_test() ->
    F0 = <<"f0">>,
    Col = [{F0, ?REPETITION_OPTIONAL}],
    Z0 = parquer_zipper:new(Col, #{}),
    Z1 = parquer_zipper:next(Z0),
    ?assertEqual({0, 0, ?undefined}, parquer_zipper:read(Z1)),
    Z2 = parquer_zipper:next(Z1),
    ?assertEqual(false, parquer_zipper:read(Z2)),
    Z3 = parquer_zipper:next(Z2),
    ?assertEqual(false, parquer_zipper:read(Z3)),
    ok.

unexpected_values_test_() ->
    F0 = <<"f0">>,
    WeirdValue = make_ref(),
    [
        {
            atom_to_list(Repetition),
            ?_assertError(
                #{
                    reason := unexpected_value,
                    value := WeirdValue,
                    path := [_]
                },
                parquer_zipper:flatten([{F0, Repetition}], WeirdValue)
            )
        }
     || Repetition <- [?REPETITION_OPTIONAL, ?REPETITION_REPEATED, ?REPETITION_REQUIRED]
    ].

missing_values_test_() ->
    F0 = <<"f0">>,
    F1 = <<"f1">>,
    F2 = <<"f2">>,
    Col0 = [{F0, ?REPETITION_REQUIRED}],
    Col1 = [{F0, ?REPETITION_REQUIRED}, {F1, ?REPETITION_REQUIRED}],
    [
        {"undefined while required",
            ?_assertError(
                #{
                    reason := missing_required_value,
                    value := ?undefined,
                    path := [_]
                },
                parquer_zipper:flatten(Col0, ?undefined)
            )},
        {"null while required",
            ?_assertError(
                #{
                    reason := missing_required_value,
                    value := ?null,
                    path := [_]
                },
                parquer_zipper:flatten(Col0, ?null)
            )},
        {"required key missing",
            ?_assertError(
                #{
                    reason := missing_required_value,
                    value := #{F1 := true},
                    path := [_]
                },
                parquer_zipper:flatten(Col0, #{F1 => true})
            )},
        {"null value (undefined) for required key (key present)",
            ?_assertError(
                #{
                    reason := missing_required_value,
                    value := #{F1 := ?undefined},
                    path := [_, _]
                },
                parquer_zipper:flatten(Col1, #{F0 => #{F1 => ?undefined}})
            )},
        {"null value (null) for required key (key present)",
            ?_assertError(
                #{
                    reason := missing_required_value,
                    value := #{F1 := ?null},
                    path := [_, _]
                },
                parquer_zipper:flatten(Col1, #{F0 => #{F1 => ?null}})
            )},
        {"required key missing (deep)",
            ?_assertError(
                #{
                    reason := missing_required_value,
                    value := #{<<"f2">> := false},
                    path := [_, _]
                },
                parquer_zipper:flatten(Col1, #{F0 => #{F2 => false}})
            )}
    ].

repeated_test_() ->
    F0 = <<"f0">>,
    Col0 = [{F0, ?REPETITION_REPEATED}],
    [
        {"key's value is undefined",
            ?_assertEqual(
                [{0, 0, ?undefined}],
                parquer_zipper:flatten(
                    Col0,
                    #{F0 => ?undefined}
                )
            )},
        {"key's value is null",
            ?_assertEqual(
                [{0, 0, ?undefined}],
                parquer_zipper:flatten(
                    Col0,
                    #{F0 => ?null}
                )
            )}
    ].
