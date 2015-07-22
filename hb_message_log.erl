-module(hb_message_log).
-author('jerray@huoban.com').

-behavior(gen_mod).

%% API:
-export([start/2,
         stop/1]).

%% Hooks:
-export([store_chat_message/3,
         iq_handler/3]).

-include("ejabberd.hrl").
-include("logger.hrl").
-include("jlib.hrl").
-include("hb_session.hrl").

-define(PROCNAME, ?MODULE).
-define(NS_NAME, <<"urn:xmpp:hb_message_log">>).

%%====================================================================
%% API
%%====================================================================
start(Host, Opts) ->
    IQDisc = gen_mod:get_opt(iqdisc, Opts, fun gen_iq_handler:check_type/1, one_queue),
    mod_disco:register_feature(Host, ?NS_NAME),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_NAME, ?MODULE, iq_handler, IQDisc).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_NAME),
    mod_disco:unregister_feature(Host, ?NS_NAME).

%%====================================================================
%% Hooks
%%====================================================================
iq_handler(Uid, Sid, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{lserver = LServer, luser = LUser} = Sid,
    #jid{luser = FromUser} = Uid,
    case {LUser, LServer, lists:member(LServer, ?MYHOSTS)} of
        {FromUser, _, true} -> process_retrieve(Uid, IQ);
        {"", _, true} -> process_retrieve(Uid, IQ);
        {"", "", _} -> process_retrieve(Uid, IQ);
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]}
    end;
iq_handler(_Uid, _Sid, #iq{type = set, sub_el = SubEl} = IQ) ->
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_FEATURE_NOT_IMPLEMENTED]}.

%%====================================================================
%% Internal functions
%%====================================================================

%%====================================================================
%% Store message
%%====================================================================
store_chat_message(Uid, Sid, Packet) ->
    Msg = parse_message(Uid, Sid, Packet),
    LServer = Uid#jid.lserver,
    process_store(LServer, Msg).

parse_message(Uid, Sid, #xmlel{name = <<"message">>, attrs = Attrs} = Packet) ->
    Type = xml:get_attr_s(<<"type">>, Attrs),
    MId = xml:get_attr_s(<<"mid">>, Attrs),
    Created = hblib:timestamp_to_integer(now()),
    Message = xml:element_to_binary(Packet),
    {
        MId,
        hblib:get_bare_jid(Sid),
        hblib:get_bare_jid(Uid),
        Message,
        Created,
        Type
    }.

process_store(LServer, {MId, Sid, Uid, Message, Created, Type}) ->
    Query = [<<"INSERT INTO `hb_message` (`mid`, `sid`, `uid`, `message`, `created`, `type`) VALUES (">>,
             <<"'">>, hblib:escape(MId), <<"',">>,
             <<"'">>, hblib:escape(Sid), <<"',">>,
             <<"'">>, hblib:escape(Uid), <<"',">>,
             <<"'">>, hblib:escape(Message), <<"',">>,
             <<"'">>, hblib:escape(Created), <<"',">>,
             <<"'">>, hblib:escape(Type), <<"'">>,
             <<");">>],
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {updated, 1} ->
            ok;
        E ->
            ?ERROR_MSG("error during query: ~p", [E])
    end.

%%====================================================================
%% Retrieve messages
%%====================================================================
process_retrieve(Uid, #iq{type = get, sub_el = SubEl} = IQ) ->
    #xmlel{attrs = Attrs} = SubEl,
    Sid = jlib:string_to_jid(xml:get_attr_s(<<"with">>, Attrs)),
    case lists:member(Uid#jid.lserver, ?MYHOSTS) of
        false ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]};
        true ->
            #xmlel{name = Name} = SubEl,
            case Name of
                <<"retrieve">> ->
                    process_retrieve(Uid, Sid, IQ);
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_FEATURE_NOT_IMPLEMENTED]}
            end
    end.

process_retrieve(Uid, Sid, #iq{sub_el = Packet} = IQ) ->
    #jid{lserver = LServer} = Uid,
    case jlib:rsm_decode(Packet) of
        none ->
            Rsm = #rsm_in{max = 30, index = 0, direction = before, id = jlib:now_to_utc_string(now())};
        Rsm -> ok
    end,

    SessionData = case hb_session:get_session(Sid#jid.lserver, LServer, Sid#jid.luser) of
        [] -> [];
        [SessionRs] -> SessionRs
    end,
    TimeConditions = case SessionData of
        #hb_session{type = chat} ->
            case dict:find(hblib:get_bare_jid(Uid), SessionData#hb_session.members) of
                {ok, _} -> {ok, <<"1">>};
                _ -> {error, deny}
            end;
        #hb_session{type = groupchat} ->
            case get_user_session_logs(Uid, Sid) of
                [] -> {error, deny};
                TimeLogs -> build_query_conditions(TimeLogs)
            end;
        _ ->
            {error, deny}
    end,
    case TimeConditions of
        {error, deny} ->
            IQ#iq{type = error, sub_el = [Packet, ?ERR_NOT_ALLOWED]};
        {abort, Reason} ->
            ?ERROR_MSG("build_query_conditions fail: ~p", [Reason]),
            IQ#iq{type = error, sub_el = [Packet, ?ERR_INTERNAL_SERVER_ERROR]};
        {ok, Conditions} ->
            case get_archive_messages(LServer, Uid, Sid, Conditions, Rsm) of
                {error, E} ->
                    ?ERROR_MSG("process retrieve error: ~p", [E]),
                    IQ#iq{type = error, sub_el = [Packet, ?ERR_INTERNAL_SERVER_ERROR]};
                Result ->
                    IQ#iq{type = result, sub_el = [Packet#xmlel{children = Result}]}
            end
    end.

get_archive_messages(LServer, _Uid, Sid, TimeConditions, #rsm_in{max = Max, index = Index, direction = Direction, id = Time}) ->
    MSid = hblib:escape(hblib:get_bare_jid(Sid)),
    QIndex = hblib:escape(jlib:integer_to_binary(Index)),
    QMax = hblib:escape(jlib:integer_to_binary(Max)),
    QTime = hblib:escape(hblib:timestamp_to_integer(jlib:datetime_string_to_timestamp(Time))),
    TimeClause = [<<" AND ">>, TimeConditions |
        case Direction of
            before -> [<<" AND `created` <= '">>, QTime, <<"' ">>];
            aft -> [<<" AND `created` >= '">>, QTime, <<"' ">>];
            _ -> []
        end],
    Header = [<<"SELECT `mid`, `sid`, `uid`, `message`, `created`, `type` FROM `hb_message`">>],
    WhereClause = [<<" WHERE `sid` = '">>, MSid, <<"'">>] ++ TimeClause,
    Footer = [<<" ORDER BY `created` DESC LIMIT ">>, QIndex, <<", ">>, QMax, <<";">>],
    Query = Header ++ WhereClause ++ Footer,
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Rs} ->
            Messages = lists:map(fun map_archive_message/1, Rs),
            RsmOut = rsm_xml(LServer, WhereClause, Index, length(Messages)),
            Messages ++ RsmOut;
        E ->
            {error, E}
    end.

map_archive_message([_Mid, _Sid, Uid, Message, Created, _Type]) ->
    xml_stream:parse_element(Message).

get_user_session_logs(Uid, Sid) ->
    #jid{lserver = LServer} = Uid,
    TUid = hblib:escape(hblib:get_bare_jid(Uid)),
    TSid = hblib:escape(hblib:get_bare_jid(Sid)),
    Query = [<<"SELECT `created`, `type` FROM `hb_user_session_log` ">>,
             <<"WHERE `uid` = '">>, TUid, <<"' AND `sid` = '">>, TSid, <<"' ">>,
             <<"ORDER BY `created` ASC;">>],
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Res} ->
            ?INFO_MSG("~n~nhb_user_session_log result: ~p~n~n", Res),
            lists:map(fun([Time, Type]) ->
                        {Time, case Type of <<"0">> -> in; _ -> out end}
                end, Res);
        _ ->
            []
    end.

build_query_conditions(TimeLogs) ->
    build_query_conditions(TimeLogs, []).

build_query_conditions([], []) ->
    {abort, <<"Can't build query conditions">>};

build_query_conditions([], Conditions) ->
    NextConditions = str:join(Conditions, <<" OR ">>),
    ?INFO_MSG("~n~n time conditions is: ~p ~n~n", [NextConditions]),
    {ok, [<<" ( ">>, NextConditions, <<" ) ">>]};

build_query_conditions([{LTime, in}], Conditions) ->
    Q = str:join([<<" created >= '">>, LTime, <<"' ">>], <<>>),
    NextConditions = [Q | Conditions],
    build_query_conditions([], NextConditions);

build_query_conditions([{LTime, out}], Conditions) ->
    build_query_conditions([], Conditions);

build_query_conditions([{LTime, in}, {RTime, out} | Tail], Conditions) ->
    Q = str:join([<<" ( created >= '">>, LTime, <<"' AND created <= '">>, RTime, <<"' ) ">>], <<>>),
    NextConditions = [Q | Conditions],
    build_query_conditions(Tail, NextConditions);

build_query_conditions([{_, _}, {RTime, in} | Tail], Conditions) ->
    build_query_conditions([{RTime, in} | Tail], Conditions).

rsm_xml(LServer, WhereClause, Index, Length) ->
    Query = [<<"SELECT COUNT(*) AS `total` FROM `hb_message` ">>] ++ WhereClause,
    Count = case catch ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [[Res]]} ->
            jlib:binary_to_integer(Res);
        _ ->
            0
    end,
    RsmOut = #rsm_out{count = Count,
                      index = Index,
                      first = jlib:integer_to_binary(Index + 1),
                      last = jlib:integer_to_binary(Index + Length)},
    jlib:rsm_encode(RsmOut).

