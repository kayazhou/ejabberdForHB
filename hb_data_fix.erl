-module(hb_data_fix).
-author('jerray@huoban.com').

-behavior(gen_mod).

%% API:
-export([start/2,
         stop/1,
         data_fix/0]).


-include("ejabberd.hrl").
-include("logger.hrl").
-include("jlib.hrl").

-define(PROCNAME, ?MODULE).
-define(NS_NAME, <<"urn:xmpp:hb_data_fix">>).
-define(HOST, <<"socket.huoban.com">>).

%%====================================================================
%% API
%%====================================================================
start(Host, Opts) ->
    ok.
stop(Host) ->
    ok.
%%====================================================================
%% Hooks
%%====================================================================
iq_handler(From, To, #iq{type = get, sub_el = SubEl} = IQ) ->
    ok;
iq_handler(_From, _To, _IQ) ->
    ok.
%%====================================================================
%% Internal functions
%%====================================================================
%%====================================================================
%% Store session
%%====================================================================
data_fix() ->
    Query = [<<"select sid  from hb_session;">>],
    case catch ejabberd_odbc:sql_query(?HOST, Query) of
        {selected, _, Rs} ->
            Messages = lists:map(fun select_message_from_message/1, Rs),
            ok;
        E ->
            {error, E}
    end.

select_message_from_message([Sid]) ->
    Sid = escape(Sid), 
    Query = [<<"select `mid`, `message`, `uid`, `type` from `hb_message`">>, 
            <<" where `sid` = '">>, Sid,  <<"' order by created asc;">>],
    case catch ejabberd_odbc:sql_query(?HOST, Query) of
        {selected, _, Rs} ->
            lists:foldl(fun(Item, PrevMId) ->
                        append_prev_mid_to_item(Item, PrevMId),
                        get_mid_from_item(Item)
                end, <<>>, Rs);
        E ->
            {error, E}
    end.

append_prev_mid_to_item([Mid, BinMessage, UId, Type], PrevMId) ->
    #xmlel{attrs = Attrs} = Message = xml_stream:parse_element(BinMessage),
    Attrs1 = [{<<"from">>, xml:get_attr_s(<<"from">>, Attrs)},
              {<<"user">>, UId},
              {<<"type">>, Type},
              {<<"mid">>, xml:get_attr_s(<<"mid">>, Attrs)},
              {<<"prevmid">>, PrevMId}],
    QMessage = escape(xml:element_to_binary(Message#xmlel{attrs = Attrs1})),
    Query = [<<"update `hb_message` set `message` = '">>, QMessage, 
            <<"' where `mid` = '">>, Mid, <<"';">>],
    ?INFO_MSG(" ~n~n hb_data_fix update sql is ~p ~n~n",[Query]),
    case catch ejabberd_odbc:sql_query(?HOST, Query) of
        {updated, _} ->
            ok;
        E ->
            ?ERROR_MSG("error during query: ~p", [E])
    end.

get_mid_from_item([Mid, Message, UId, Type]) ->
    Mid.

store_read_flag(Uid, SessionJID) -> 
    #jid{lserver = LServer} = Uid,
    Userid = escape(get_bare_jid(Uid)),
    Sid = escape(get_bare_jid(SessionJID)), 
    Query = [<<"update `hb_user_session` set `unread` = '0'">>, 
            <<" where `sid` = '">>, Sid, 
            <<"' and `uid` = '">>, Userid, <<"';">>],
    case catch ejabberd_odbc:sql_query(LServer, Query) of
        {updated, _} ->
            ok;
        E ->
            ?ERROR_MSG("error during query: ~p", [E])
    end.
%%====================================================================
%% Retrieve messages
%%====================================================================
%%====================================================================
%% Helper functions
%%====================================================================
get_server_host(Host) ->
    [_ | Tokens] = str:tokens(Host, <<".">>),
    str:join(Tokens, <<".">>).

escape(Value) ->
    ejabberd_odbc:escape(Value).

get_bare_jid(#jid{} = JID) ->
    jlib:jid_to_string(jlib:jid_remove_resource(JID));
get_bare_jid({_U, _S, _R} = JID) ->
    jlib:jid_to_string(jlib:jid_remove_resource(JID)).

%%====================================================================
%% Datetime handling
%%====================================================================
parse_datetime(TimeStr, sql) ->
    [Date, Time] = str:tokens(TimeStr, <<" ">>),
    TimeStr1 = str:join([Date, <<"T">>, Time, <<"Z">>], <<>>),
    parse_datetime(TimeStr1).

parse_datetime(TimeStr) ->
    [Date, Time] = str:tokens(TimeStr, <<"T">>),
    D = parse_date(Date),
    {T, MS, TZH, TZM} = parse_time(Time),
    S = calendar:datetime_to_gregorian_seconds({D, T}),
    S1 = calendar:datetime_to_gregorian_seconds({{1970, 1,
                          1},
                         {0, 0, 0}}),
    Seconds = S - S1 - TZH * 60 * 60 - TZM * 60,
    {Seconds div 1000000, Seconds rem 1000000, MS}.

% yyyy-mm-dd
parse_date(Date) ->
    [Y, M, D] = str:tokens(Date, <<"-">>),
    Date1 = {jlib:binary_to_integer(Y), jlib:binary_to_integer(M),
         jlib:binary_to_integer(D)},
    case calendar:valid_date(Date1) of
      true -> Date1;
      _ -> false
    end.

% hh:mm:ss[.sss]TZD
parse_time(Time) ->
    case str:str(Time, <<"Z">>) of
      0 -> parse_time_with_timezone(Time);
      _ ->
      [T | _] = str:tokens(Time, <<"Z">>),
      {TT, MS} = parse_time1(T),
      {TT, MS, 0, 0}
    end.

parse_time_with_timezone(Time) ->
    case str:str(Time, <<"+">>) of
      0 ->
      case str:str(Time, <<"-">>) of
        0 -> false;
        _ -> parse_time_with_timezone(Time, <<"-">>)
      end;
      _ -> parse_time_with_timezone(Time, <<"+">>)
    end.

parse_time_with_timezone(Time, Delim) ->
    [T, TZ] = str:tokens(Time, Delim),
    {TZH, TZM} = parse_timezone(TZ),
    {TT, MS} = parse_time1(T),
    case Delim of
      <<"-">> -> {TT, MS, -TZH, -TZM};
      <<"+">> -> {TT, MS, TZH, TZM}
    end.

parse_timezone(TZ) ->
    [H, M] = str:tokens(TZ, <<":">>),
    {[H1, M1], true} = check_list([{H, 12}, {M, 60}]),
    {H1, M1}.

parse_time1(Time) ->
    [HMS | T] = str:tokens(Time, <<".">>),
    MS = case T of
       [] -> 0;
       [Val] -> jlib:binary_to_integer(str:left(Val, 6, $0))
     end,
    [H, M, S] = str:tokens(HMS, <<":">>),
    {[H1, M1, S1], true} = check_list([{H, 24}, {M, 60},
                       {S, 60}]),
    {{H1, M1, S1}, MS}.

check_list(List) ->
    lists:mapfoldl(fun ({L, N}, B) ->
               V = jlib:binary_to_integer(L),
               if (V >= 0) and (V =< N) -> {V, B};
                  true -> {false, false}
               end
           end,
           true, List).
