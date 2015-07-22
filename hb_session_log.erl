-module(hb_session_log).
-author('jerray@huoban.com').

-behavior(gen_mod).

%% API:
-export([start/2,
         stop/1,
         create_user_session/2,
         create_user_session/3,
         create_user_session_for_chat/3,
         update_user_session/3,
         delete_user_session/3,
         store_read_flag/2,
         get_exist_members/2,
         get_user_session_info/2,
         store_session_flag/4]).

%% Hooks:
-export([iq_handler/3]).

-include("ejabberd.hrl").
-include("logger.hrl").
-include("jlib.hrl").

-define(PROCNAME, ?MODULE).
-define(NS_NAME, <<"urn:xmpp:hb_session_log">>).

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
iq_handler(From, To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{lserver = LServer} = From,
    case lists:member(LServer, ?MYHOSTS) of
        true -> process_retrieve(From, To, IQ);
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]}
    end;
iq_handler(_From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_FEATURE_NOT_IMPLEMENTED]}.

%%====================================================================
%% Internal functions
%%====================================================================
%%====================================================================
%% Store session
%%====================================================================

%% 只创建记录，不激活
create_user_session(SessionJID, Members) ->
    create_user_session(SessionJID, Members, false).

create_user_session(SessionJID, Members, IsActive) ->
    #jid{lserver = LServer} = SessionJID,
    Host = get_server_host(LServer),
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Changed = hblib:escape(hblib:timestamp_to_integer(now())),

    F = fun() ->
        lists:foreach(fun(UserID) ->
                    process_create(Sid, UserID, Changed, IsActive)
            end, Members)
    end,
    case catch ejabberd_odbc:sql_transaction(Host, F) of
        {atomic, Rs} ->
            ?INFO_MSG("create user session ok, ~p", [Rs]),
            ok;
        {aborted, Err} ->
            ?ERROR_MSG("create user session error, ~p", [Err]),
            error
    end.

create_user_session_for_chat(SessionJID, Members, Creator) ->
    #jid{lserver = LServer} = SessionJID,
    Host = get_server_host(LServer),
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Changed = hblib:escape(hblib:timestamp_to_integer(now())),
    F = fun() ->
        lists:foreach(fun(UserID) ->
                    process_create_for_chat(Sid, UserID, Changed, Creator == UserID)
            end, Members)
    end,
    case catch ejabberd_odbc:sql_transaction(Host, F) of
        {atomic, Rs} ->
            ?INFO_MSG("create user session ok, ~p", [Rs]),
            ok;
        {aborted, Err} ->
            ?ERROR_MSG("create user session error, ~p", [Err]),
            error
    end.

process_create_for_chat(Sid, UserID, Changed, IsActive) ->
    Uid = hblib:escape(hblib:get_bare_jid(UserID)),
    case IsActive of
        true -> Flags = [<<"'0', '0', '1', '1');">>];
        _ -> Flags = [<<"'0', '0', '1', '0');">>]
    end,
    CheckQuery = [<<"SELECT `sid` FROM `hb_user_session` ">>,
                  <<" WHERE `sid` = '">>, Sid, <<"' AND `uid` = '">>, Uid, <<"';">>],
    SessionQuery = case ejabberd_odbc:sql_query_t(CheckQuery) of
        {selected, _, []} ->
            [<<"INSERT INTO `hb_user_session` (`uid`, `sid`, `created`, `changed`, `isdeleted`, `unread`, `notify`, `isactive`) VALUES (">>,
             <<"'">>, Uid, <<"',">>, <<"'">>, Sid, <<"',">>, <<"'">>, Changed, <<"',">>, <<"'">>, Changed, <<"',">>] ++ Flags;
        {selected, _, [_]} ->
            null;
        Err ->
            ?INFO_MSG("database error while query ~p, error: ~p", [CheckQuery, Err]),
            throw({error, Err})
    end,

    case SessionQuery of
        null -> ok;
        _ ->
            {updated, _} = ejabberd_odbc:sql_query_t(SessionQuery)
    end.


process_create(Sid, UserID, Changed, IsActive) ->
    Uid = hblib:escape(hblib:get_bare_jid(UserID)),
    case IsActive of
        true -> Flags = [<<"'0', '0', '1', '1');">>];
        _ -> Flags = [<<"'0', '0', '1', '0');">>]
    end,
    CheckQuery = [<<"SELECT `isquit` FROM `hb_user_session` ">>,
                  <<" WHERE `sid` = '">>, Sid, <<"' AND `uid` = '">>, Uid, <<"' AND `isquit` = 1;">>],

    SessionQuery = case ejabberd_odbc:sql_query_t(CheckQuery) of
        {selected, _, []} ->
            [<<"INSERT INTO `hb_user_session` (`uid`, `sid`, `created`, `changed`, `isdeleted`, `unread`, `notify`, `isactive`) VALUES (">>,
             <<"'">>, Uid, <<"',">>, <<"'">>, Sid, <<"',">>, <<"'">>, Changed, <<"',">>, <<"'">>, Changed, <<"',">>] ++ Flags;
        {selected, _, [_]} ->
            [<<"UPDATE `hb_user_session` SET `isquit` = 0 ">>,
             <<" WHERE `sid` = '">>, Sid, <<"' AND `uid` = '">>, Uid, <<"';">>];
        Err ->
            ?INFO_MSG("database error while query ~p, error: ~p", [CheckQuery, Err]),
            throw({error, Err})
    end,

    case SessionQuery of
        null -> ok;
        _ ->
            LogQuery = [<<"INSERT INTO `hb_user_session_log` (`uid`, `sid`, `type`, `created`) VALUES (">>,
                        <<"'">>, Uid, <<"',">>, <<"'">>, Sid, <<"', '0', '">>, Changed, <<"');">>],
            {updated, _} = ejabberd_odbc:sql_query_t(SessionQuery),
            {updated, _} = ejabberd_odbc:sql_query_t(LogQuery)
    end.


%% 发送消息时更新记录
update_user_session(SessionJID, Members, #jid{} = FromID) ->
    #jid{lserver = LServer} = SessionJID,
    Host = get_server_host(LServer),
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Changed = hblib:escape(hblib:timestamp_to_integer(now())),
    lists:foreach(fun(UserID) ->
                    process_store(Host, Sid, UserID, FromID, Changed)
                end, Members).

process_store(Host, Sid, UserID, FromID, Changed) ->
    Uid = hblib:escape(hblib:get_bare_jid(UserID)),
    Query = [<<"UPDATE `hb_user_session` SET ">>,
             <<"`changed` = CASE WHEN `notify` = '1' THEN '">>, Changed, <<"' ELSE `changed` END, ">>,
             case UserID == FromID of
                true -> <<>>;
                false -> <<"`unread` = `unread` + 1, ">>
             end,
             <<"`isdeleted` = 0, isactive = 1 ">>,
             <<" WHERE `sid` = '">>, Sid, <<"' AND `uid` = '">>, Uid, <<"';">>],
    case catch ejabberd_odbc:sql_query(Host, Query) of
        {updated, _} ->
            ok;
        E ->
            ?ERROR_MSG("error during query: ~p", [E])
    end.

delete_user_session(SessionJID, Members, {Name, RemainMembers, MessageEl}) ->
    #jid{lserver = LServer} = SessionJID,
    Host = get_server_host(LServer),
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Changed = hblib:escape(hblib:timestamp_to_integer(now())),

    SName = hblib:escape(Name),
    SRMembers = hblib:escape(str:join(dict:fetch_keys(RemainMembers), <<",">>)),
    SMessage = case MessageEl of
        null -> <<"">>;
        #xmlel{} -> hblib:escape(xml:element_to_binary(MessageEl))
    end,

    F = fun() ->
        lists:foreach(fun(UserID) ->
                        process_delete(Sid, UserID, Changed, {SName, SRMembers, SMessage})
                    end, Members)
    end,
    case catch ejabberd_odbc:sql_transaction(Host, F) of
        {atomic, Rs} ->
            ?INFO_MSG("delete user session ok, ~p", [Rs]),
            ok;
        {aborted, Err} ->
            ?ERROR_MSG("delete user session error, ~p", [Err]),
            error
    end.

process_delete(Sid, UserID, Changed, {Name, Members, Message}) ->
    Uid = hblib:escape(hblib:get_bare_jid(UserID)),
    SessionQuery = [<<"UPDATE `hb_user_session` SET `isquit` = 1, ">>,
                    <<"`snapshot_name` = CASE WHEN `isactive` = 1 THEN '">>, Name, <<"' ELSE `snapshot_name` END, ">>,
                    <<"`snapshot_members` = CASE WHEN `isactive` = 1 THEN '">>, Members, <<"' ELSE `snapshot_members` END, ">>,
                    <<"`snapshot_message` = CASE WHEN `isactive` = 1 THEN '">>, Message, <<"' ELSE `snapshot_message` END ">>,
                    <<"WHERE `sid` = '">>, Sid, <<"' AND `uid` = '">>, Uid, <<"';">>],
    LogQuery = [<<"INSERT INTO `hb_user_session_log` (`uid`, `sid`, `type`, `created`) VALUES (">>,
                <<"'">>, Uid, <<"',">>, <<"'">>, Sid, <<"', '1', '">>, Changed, <<"');">>],
    {updated, _} = ejabberd_odbc:sql_query_t(SessionQuery),
    {updated, _} = ejabberd_odbc:sql_query_t(LogQuery).

get_user_session_info(#jid{lserver = Host} = UserJID, SessionJID) ->
    UId = hblib:escape(hblib:get_bare_jid(UserJID)),
    SId = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Query = [<<"select `unread`, `notify` from `hb_user_session` ">>,
             <<"where `sid` = '">>, SId, <<"' ">>,
             <<"and `uid` = '">>, UId, <<"';">>],
    case catch ejabberd_odbc:sql_query(Host, Query) of
        {selected, _, [[Unread, Notify]]} ->
            {ok, {Unread, Notify}};
        E ->
            {error, E}
    end.

get_exist_members(Host, SessionJID) ->
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Query = [<<"SELECT `uid` FROM `hb_user_session`">>,
            <<" WHERE `sid` = '">>, Sid,  <<"' AND `isactive` = 1 AND `isquit` = 0;">>],
    case catch ejabberd_odbc:sql_query(Host, Query) of
        {selected, _, Rs} ->
            lists:foldl(fun([UID], Acc) ->
                            [jlib:string_to_jid(UID) | Acc]
                        end, [], Rs);
        E ->
            {error, E}
    end.

store_read_flag(Uid, SessionJID) ->
    #jid{lserver = LServer} = Uid,
    Userid = hblib:escape(hblib:get_bare_jid(Uid)),
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Query = [<<"update `hb_user_session` set `unread` = '0'">>,
            <<" where `sid` = '">>, Sid,
            <<"' and `uid` = '">>, Userid, <<"';">>],
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {updated, _} ->
            ok;
        E ->
            ?ERROR_MSG("error during query: ~p", [E])
    end.

store_session_flag(Uid, SessionJID, isdeleted, Value) ->
    #jid{lserver = LServer} = Uid,
    Userid = hblib:escape(hblib:get_bare_jid(Uid)),
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    IsDeleted = hblib:escape(Value),
    Query = [<<"update `hb_user_session` set `isdeleted` = '">>, IsDeleted,
            <<"' where `sid` = '">>, Sid,
            <<"' and `uid` = '">>, Userid, <<"';">>],
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {updated, _} ->
            ok;
        E ->
            ?ERROR_MSG("error during query: ~p", [E])
    end;
store_session_flag(Uid, SessionJID, notify, Value) ->
    #jid{lserver = LServer} = Uid,
    Userid = hblib:escape(hblib:get_bare_jid(Uid)),
    Sid = hblib:escape(hblib:get_bare_jid(SessionJID)),
    Notify = hblib:escape(Value),
    Query = [<<"update `hb_user_session` set `notify` = '">>, Notify,
            <<"' where `sid` = '">>, Sid,
            <<"' and `uid` = '">>, Userid, <<"';">>],
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {updated, _} ->
            ok;
        E ->
            ?ERROR_MSG("error during query: ~p", [E])
    end.
%====================================================================
%% Retrieve messages
%%====================================================================
process_retrieve(From, To, #iq{type = get, sub_el = SubEl} = IQ) ->
    case lists:member(From#jid.lserver, ?MYHOSTS) of
        false ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]};
        true ->
            #xmlel{name = Name} = SubEl,
            case Name of
                <<"retrieve">> ->
                    case process_retrieve_list(From, To, SubEl) of
                        error ->
                            IQ#iq{type = error, sub_el = [SubEl, ?ERR_INTERNAL_SERVER_ERROR]};
                        Result ->
                            IQ#iq{type = result, sub_el = [SubEl#xmlel{children = Result}]}
                    end;
                <<"retrievegroupchat">> ->
                    case process_retrieve_groupchat(From, To, SubEl) of
                        error ->
                            IQ#iq{type = error, sub_el = [SubEl, ?ERR_INTERNAL_SERVER_ERROR]};
                        Result ->
                            IQ#iq{type = result, sub_el = [SubEl#xmlel{children = Result}]}
                    end;
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_FEATURE_NOT_IMPLEMENTED]}
            end
    end.
process_retrieve_list(From, _To, #xmlel{} = Packet) ->
    #jid{lserver = LServer} = From,
    case jlib:rsm_decode(Packet) of
        none ->
            Rsm = #rsm_in{max = 500, index = 0, direction = before, id = jlib:now_to_utc_string(now())};
        Rsm -> ok
    end,
    case get_archive_sessions_list(LServer, From, Rsm) of
        {error, E} ->
            ?ERROR_MSG("process retrieve error: ~p", [E]),
            error;
        Result -> Result
    end.

process_retrieve_groupchat(From, _To, #xmlel{} = Packet) ->
    #jid{lserver = LServer} = From,
    case jlib:rsm_decode(Packet) of
        none ->
            Rsm = #rsm_in{max = 500, index = 0, direction = before, id = jlib:now_to_utc_string(now())};
        Rsm -> ok
    end,
    case get_archive_sessions_groupchat(LServer, From, Rsm) of
        {error, E} ->
            ?ERROR_MSG("process retrieve error: ~p", [E]),
            error;
        Result -> Result
    end.

get_archive_sessions_list(LServer, User, #rsm_in{max = Max, index = Index, direction = Direction, id = Time}) ->
    QUser = hblib:escape(hblib:get_bare_jid(User)),
    QIndex = hblib:escape(jlib:integer_to_binary(Index)),
    QMax = hblib:escape(jlib:integer_to_binary(Max)),
    QTime = hblib:escape(hblib:timestamp_to_integer(jlib:datetime_string_to_timestamp(Time))),
    TimeClause =
        case Direction of
            before -> [<<" AND `u`.`changed` <= '">>, QTime, <<"' ">>];
            aft -> [<<" AND `u`.`changed` >= '">>, QTime, <<"' ">>];
            _ -> []
        end,
    Header = [<<"SELECT `u`.`uid`, `u`.`sid`, `u`.`created`, `u`.`changed`,">>,
              <<" `u`.`isdeleted`, `u`.`unread`, `u`.`notify`, ">>,
              <<"CASE WHEN `u`.`isquit` = 1 THEN `u`.`snapshot_name` ELSE `s`.`name` END, ">>,
              <<"`s`.`type`, ">>,
              <<"CASE WHEN `u`.`isquit` = 1 THEN `u`.`snapshot_message` ELSE `s`.`last_message` END, ">>,
              <<"CASE WHEN `u`.`isquit` = 1 THEN `u`.`snapshot_members` ELSE `s`.`members` END, ">>,
              <<"`u`.`isquit` ">>,
              <<"FROM `hb_user_session` AS `u`,`hb_session` AS `s` ">>],
    WhereClause = [<<" WHERE isdeleted = 0 AND isactive = 1 AND `u`.`sid` = `s`.`sid` AND  `u`.`uid` = '">>, QUser, <<"'">>] ++ TimeClause,
    Footer = [<<" ORDER BY `u`.`changed` DESC LIMIT ">>, QIndex, <<", ">>, QMax, <<";">>],

    Query = Header ++ WhereClause  ++ Footer,
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Rs} ->
            Sessions = lists:map(fun map_archive_session_list/1, Rs),
            RsmOut = rsm_xml(LServer, WhereClause , Index, length(Sessions)),
            Sessions ++ RsmOut;
        E ->
            {error, E}
    end.

get_archive_sessions_groupchat(LServer, User, #rsm_in{max = Max, index = Index, direction = Direction, id = Time}) ->
    QUser = hblib:escape(hblib:get_bare_jid(User)),
    QIndex = hblib:escape(jlib:integer_to_binary(Index)),
    QMax = hblib:escape(jlib:integer_to_binary(Max)),
    QTime = hblib:escape(hblib:timestamp_to_integer(jlib:datetime_string_to_timestamp(Time))),
    TimeClause =
        case Direction of
            before -> [<<" AND `u`.`changed` <= '">>, QTime, <<"' ">>];
            aft -> [<<" AND `u`.`changed` >= '">>, QTime, <<"' ">>];
            _ -> []
        end,
    Header = [<<"SELECT `u`.`uid`, `u`.`sid`, `u`.`changed`, ">>,
              <<"`u`.`unread`, ">>,
              <<"CASE WHEN `u`.`isquit` = 1 THEN `u`.`snapshot_name` ELSE `s`.`name` END, ">>,
              <<"`s`.`type`, ">>,
              <<"CASE WHEN `u`.`isquit` = 1 THEN `u`.`snapshot_message` ELSE `s`.`last_message` END, ">>,
              <<"CASE WHEN `u`.`isquit` = 1 THEN `u`.`snapshot_members` ELSE `s`.`members` END, ">>,
              <<"`u`.`isquit` ">>,
              <<"FROM `hb_user_session` AS `u`,`hb_session` AS `s` ">>],
    WhereClause = [<<" WHERE `s`.`type` = 'groupchat' AND `u`.`sid` = `s`.`sid` AND  `u`.`uid` = '">>, QUser, <<"'">>] ++ TimeClause,
    Footer = [<<" ORDER BY `u`.`changed` DESC LIMIT ">>, QIndex, <<", ">>, QMax, <<";">>],
    Query = Header ++ WhereClause  ++ Footer,
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Rs} ->
            Sessions = lists:map(fun map_archive_session_groupchat/1, Rs),
            RsmOut = rsm_xml(LServer, WhereClause , Index, length(Sessions)),
            Sessions ++ RsmOut;
        E ->
            {error, E}
    end.

map_archive_session_list([Uid, Sid, Created, Changed, Isdeleted, Unread, Notify, Name, Type, LastMessage, Members, IsQuit]) ->
    MessageEl = case LastMessage of
        null -> [];
        <<"">> -> [];
        _ -> [xml_stream:parse_element(LastMessage)]
    end,
    MemberEls = case Type of
        <<"chat">> ->
            [#xmlel{name = <<"member">>, attrs = [{<<"jid">>, R}]} || R <- str:tokens(Members, <<",">>), R /= Uid];
        <<"groupchat">> ->
            [#xmlel{name = <<"member">>, attrs = [{<<"jid">>, R}]} || R <- str:tokens(Members, <<",">>)]
    end,
    #xmlel{name = <<"item">>,
           attrs = [{<<"jid">>, Sid},
                    {<<"name">>, Name},
                    {<<"type">>, Type},
                    {<<"changed">>, jlib:now_to_utc_string(hblib:integer_to_timestamp(Changed))},
                    {<<"notify">>, Notify},
                    {<<"quit">>, IsQuit},
                    {<<"unread">>, Unread}],
           children = MessageEl ++ MemberEls}.

map_archive_session_groupchat([Uid, Sid, Changed, Unread, Name, Type, LastMessage, Members, IsQuit]) ->
    MessageEl = case LastMessage of
        null -> [];
        <<"">> -> [];
        _ -> [xml_stream:parse_element(LastMessage)]
    end,
    MemberEls = case Type of
        <<"chat">> ->
            [#xmlel{name = <<"member">>, attrs = [{<<"jid">>, R}]} || R <- str:tokens(Members, <<",">>), R /= Uid];
        <<"groupchat">> ->
            [#xmlel{name = <<"member">>, attrs = [{<<"jid">>, R}]} || R <- str:tokens(Members, <<",">>)]
    end,
    #xmlel{name = <<"item">>,
           attrs = [{<<"jid">>, Sid},
                    {<<"name">>, Name},
                    {<<"type">>, Type},
                    {<<"quit">>, IsQuit},
                    {<<"unread">>, Unread}],
           children = MessageEl ++ MemberEls}.

rsm_xml(LServer, WhereClause, Index, Length) ->
    Query = [<<"SELECT count(*) AS total FROM `hb_user_session` AS `u`, `hb_session` AS `s` ">>] ++ WhereClause,
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

%%====================================================================
%% Helper functions
%%====================================================================
get_server_host(Host) ->
    [_ | Tokens] = str:tokens(Host, <<".">>),
    str:join(Tokens, <<".">>).
