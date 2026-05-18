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
-module(parquer_zipper).

%% API
-export([
    new/2,
    reset/2,
    next/1,
    read/1,
    next_read/1,
    flatten/2
]).

%% Debug only
-export([inspect/1]).

-export_type([t/0, data/0]).

-include("parquer.hrl").

-moduledoc """
This is a "zipper" that traverses a record while projecting a specific column and keeping
track of definition and repetition levels.

It's not a general zipper because it only moves forward, and keeps track of the levels.
""".

-compile(
    {inline, [
        next/1, down/1, deep_down/1, read/1, backtrack/1, repetition_level/1, definition_level/1
    ]}
).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(END, 'end').

-record(zipper, {
    node,
    definition_level = 0,
    repetition_level = 0,
    max_definition_level,
    max_repetition_level,
    context,
    path
}).

-record(context, {
    left = [],
    right = [],
    parent_nodes = [],
    parent_path = [],
    parent_contexts = []
}).

-opaque t() :: #zipper{}.

-type repetition() :: ?REPETITION_OPTIONAL | ?REPETITION_REPEATED | ?REPETITION_REQUIRED.
-type data() :: map() | list().
-type repetition_level() :: non_neg_integer().
-type definition_level() :: non_neg_integer().
-type value() :: {repetition_level(), definition_level(), term()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec reset(t(), data()) -> t().
reset(Z0, Data) ->
    Z0#zipper{node = Data}.

-spec new([{term(), repetition()}], data()) -> t().
new(KeyRepetitions, Data) ->
    #zipper{
        node = Data,
        path = KeyRepetitions,
        max_definition_level = definition_level(KeyRepetitions),
        max_repetition_level = repetition_level(KeyRepetitions),
        context = ?undefined
    }.

-spec flatten([{term(), repetition()}], data()) -> [value()].
flatten(KeyRepetitions, Data) ->
    Z0 = new(KeyRepetitions, Data),
    do_flatten(Z0, []).

-spec read(t()) -> false | value().
read(#zipper{node = ?END}) ->
    false;
read(#zipper{} = Z) ->
    #zipper{
        node = Node,
        max_definition_level = MaxDefLevel,
        repetition_level = RepLevel,
        definition_level = DefLevel
    } = Z,
    case DefLevel == MaxDefLevel of
        true ->
            {RepLevel, DefLevel, Node};
        false ->
            {RepLevel, DefLevel, ?undefined}
    end.

-spec next(t()) -> t().
next(#zipper{node = ?END} = Z) ->
    Z;
next(#zipper{path = KeyRepetitions, context = Context} = Z0) ->
    case down(Z0) of
        #zipper{} = Z1 ->
            deep_down(Z1);
        false when KeyRepetitions /= [], Context == undefined ->
            %% This is the first `next`, but the record is completely blank.  Return at least
            %% one `?undefined` for the whole record on the next `read`.
            Z0#zipper{node = ?undefined, context = #context{}};
        false ->
            case right(Z0) of
                #zipper{} = Z1 ->
                    deep_down(Z1);
                false ->
                    backtrack(Z0)
            end
    end.

-spec next_read(?undefined | t()) -> ?undefined | {value(), ?undefined | t()}.
next_read(?undefined) ->
    ?undefined;
next_read(#zipper{path = KeyRepetitions, context = Context} = Z0) ->
    case down(Z0) of
        #zipper{} = Z1 ->
            Z2 = deep_down(Z1),
            {read(Z2), Z2};
        false when KeyRepetitions /= [], Context == undefined ->
            %% This is the first `next`, but the record is completely blank.  Return at least
            %% one `?undefined` for the whole record.
            Z1 = Z0#zipper{node = ?undefined, context = #context{}},
            {read(Z1), ?undefined};
        false ->
            case right(Z0) of
                #zipper{} = Z1 ->
                    Z2 = deep_down(Z1),
                    {read(Z2), Z2};
                false ->
                    case backtrack(Z0) of
                        #zipper{node = ?END} ->
                            ?undefined;
                        Z1 ->
                            {read(Z1), Z1}
                    end
            end
    end.

%%------------------------------------------------------------------------------
%% Debug only
%%------------------------------------------------------------------------------

inspect(#zipper{} = Z) ->
    #zipper{
        node = Node,
        definition_level = DefLevel,
        repetition_level = RepLevel,
        context = Context,
        path = Path
    } = Z,
    #{
        node => Node,
        definition_level => DefLevel,
        repetition_level => RepLevel,
        context => Context /= undefined andalso inspect(Context),
        path => Path
    };
inspect(#context{} = C) ->
    #context{
        left = Left,
        right = Right,
        parent_nodes = PNodes,
        parent_path = PPath,
        parent_contexts = PContexts
    } = C,
    #{
        left => Left,
        right => Right,
        parent_nodes => PNodes,
        parent_path => PPath,
        parent_contexts => lists:map(fun inspect/1, PContexts)
    }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec deep_down(t()) -> t().
deep_down(#zipper{} = Z0) ->
    case down(Z0) of
        false ->
            Z0;
        #zipper{} = Z1 ->
            deep_down(Z1)
    end.

push_context_down(PNode, KeyRepetition, Rights, undefined) ->
    #context{
        left = [],
        right = Rights,
        parent_nodes = [PNode],
        parent_path = [KeyRepetition],
        parent_contexts = []
    };
push_context_down(PNode, KeyRepetition, Rights, Context0) ->
    #context{
        parent_nodes = PNodes0,
        parent_path = PPath0,
        parent_contexts = PContexts
    } = Context0,
    #context{
        left = [],
        right = Rights,
        parent_nodes = [PNode | PNodes0],
        parent_path = [KeyRepetition | PPath0],
        parent_contexts = [Context0 | PContexts]
    }.

definition_level_of(?REPETITION_REQUIRED) -> 0;
definition_level_of(_) -> 1.

repetition_level_of(?REPETITION_REPEATED) -> 1;
repetition_level_of(_) -> 0.

definition_level(Path) ->
    lists:sum([definition_level_of(Repetition) || {_Key, Repetition} <- Path]).

repetition_level(#context{parent_path = PPath}) ->
    repetition_level(PPath);
repetition_level(Path) ->
    lists:sum([repetition_level_of(Repetition) || {_Key, Repetition} <- Path]).

-spec backtrack(t()) -> t() | undefined.
backtrack(Z0) ->
    case up(Z0) of
        #zipper{} = Z1 ->
            case right(Z1) of
                #zipper{} = Z2 ->
                    deep_down(Z2);
                false ->
                    backtrack(Z1)
            end;
        false ->
            Z0#zipper{node = ?END}
    end.

do_flatten(Z0, Acc) ->
    Z1 = next(Z0),
    case read(Z1) of
        false ->
            lists:reverse(Acc);
        Res ->
            do_flatten(Z1, [Res | Acc])
    end.

raise_missing_value(#zipper{context = C}, KeyRepetition, Value) ->
    Path0 = parent_path(C),
    Path = lists:reverse([KeyRepetition | Path0]),
    Error = #{
        reason => missing_required_value,
        path => Path,
        value => Value
    },
    error(Error).

raise_unexpected_value(#zipper{context = C}, KeyRepetition, Data) ->
    Path0 = parent_path(C),
    Path = lists:reverse([KeyRepetition | Path0]),
    Error = #{
        reason => unexpected_value,
        path => Path,
        value => Data
    },
    error(Error).

parent_path(undefined) -> [];
parent_path(#context{parent_path = PPath}) -> PPath.

-spec right(t()) -> false | t().
right(#zipper{node = N, context = #context{right = [R | Rs], left = Ls} = C0} = Z0) ->
    %% Once we move right, need to update repetition level.
    Z0#zipper{
        node = R,
        repetition_level = repetition_level(C0),
        context = C0#context{right = Rs, left = [N | Ls]}
    };
right(_) ->
    false.

-spec down(t()) -> false | t().
down(#zipper{node = ?END}) ->
    false;
down(
    #zipper{
        path = [],
        node = FinalNode,
        context = #context{parent_path = [?REPETITION_REQUIRED = KR | _]}
    } = Z0
) when
    FinalNode == ?undefined;
    FinalNode == ?null
->
    raise_missing_value(Z0, KR, FinalNode);
down(#zipper{path = []}) ->
    false;
down(#zipper{node = Node, path = [{Key, ?REPETITION_REQUIRED} = KR | KeyRest], context = C0} = Z0) ->
    case Node of
        ?undefined ->
            raise_missing_value(Z0, KR, Node);
        ?null ->
            raise_missing_value(Z0, KR, Node);
        #{Key := InnerNode} when
            (InnerNode == ?undefined orelse InnerNode == ?null) andalso KeyRest == []
        ->
            raise_missing_value(Z0, KR, Node);
        #{Key := InnerNode} when InnerNode == ?undefined; InnerNode == ?null ->
            %% They key itself is defined.
            C1 = push_context_down(Node, KR, [], C0),
            Z0#zipper{
                node = InnerNode,
                path = KeyRest,
                context = C1
            };
        #{Key := InnerNode} ->
            C1 = push_context_down(Node, KR, [], C0),
            Z0#zipper{
                node = InnerNode,
                path = KeyRest,
                context = C1
            };
        #{} ->
            raise_missing_value(Z0, KR, Node);
        _ ->
            raise_unexpected_value(Z0, KR, Node)
    end;
down(#zipper{node = Node, path = [{Key, ?REPETITION_REPEATED} = KR | KeyRest], context = C0} = Z0) ->
    case Node of
        ?undefined ->
            false;
        ?null ->
            false;
        [] ->
            C1 = push_context_down(Node, KR, [], C0),
            InnerNode = ?undefined,
            Z0#zipper{
                node = InnerNode,
                path = KeyRest,
                context = C1
            };
        [InnerNode | Rights] ->
            C1 = push_context_down(Node, KR, Rights, C0),
            Z0#zipper{
                node = InnerNode,
                definition_level = Z0#zipper.definition_level + 1,
                path = KeyRest,
                context = C1
            };
        #{Key := InnerNode0} ->
            {InnerNode, Rights, DefLevel} =
                case InnerNode0 of
                    ?undefined ->
                        {?undefined, [], Z0#zipper.definition_level};
                    ?null ->
                        {?undefined, [], Z0#zipper.definition_level};
                    [] ->
                        {?undefined, [], Z0#zipper.definition_level};
                    [InnerNode1 | Rights1] ->
                        {InnerNode1, Rights1, Z0#zipper.definition_level + 1}
                end,
            C1 = push_context_down(Node, KR, Rights, C0),
            Z0#zipper{
                node = InnerNode,
                definition_level = DefLevel,
                path = KeyRest,
                context = C1
            };
        #{} ->
            false;
        _ ->
            raise_unexpected_value(Z0, KR, Node)
    end;
down(#zipper{node = Node, path = [{Key, ?REPETITION_OPTIONAL} = KR | KeyRest], context = C0} = Z0) ->
    case Node of
        ?undefined ->
            false;
        ?null ->
            false;
        #{Key := InnerNode} when InnerNode == ?undefined; InnerNode == ?null ->
            C1 = push_context_down(Node, KR, [], C0),
            Z0#zipper{
                node = InnerNode,
                path = KeyRest,
                context = C1
            };
        #{Key := InnerNode} ->
            C1 = push_context_down(Node, KR, [], C0),
            Z0#zipper{
                node = InnerNode,
                definition_level = Z0#zipper.definition_level + 1,
                path = KeyRest,
                context = C1
            };
        #{} ->
            C1 = push_context_down(Node, KR, [], C0),
            InnerNode = ?undefined,
            Z0#zipper{
                node = InnerNode,
                path = KeyRest,
                context = C1
            };
        _ ->
            raise_unexpected_value(Z0, KR, Node)
    end.

-spec up(t()) -> false | t().
up(#zipper{node = ?END}) ->
    false;
up(#zipper{context = ?undefined}) ->
    false;
up(#zipper{context = #context{parent_contexts = []}}) ->
    false;
up(#zipper{path = Path, context = #context{parent_nodes = [PNode | _]} = C0} = Z0) ->
    #context{
        parent_path = [KeyRepetition = {_, Repetition} | _],
        parent_contexts = [PContext | _]
    } = C0,
    DefLevel =
        case Z0#zipper.node of
            ?undefined ->
                Z0#zipper.definition_level;
            ?null ->
                Z0#zipper.definition_level;
            _ ->
                Z0#zipper.definition_level - definition_level_of(Repetition)
        end,
    Z0#zipper{
        node = PNode,
        definition_level = DefLevel,
        path = [KeyRepetition | Path],
        context = PContext
    }.
