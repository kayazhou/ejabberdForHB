-module(hb_session).

-behaviour(gen_fsm).

%% API
-export([start_link/4,
         start/4]).

%% gen_fsm callbacks
-export([init/1,
         normal_state/2,
         normal_state/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([session_exists/3,
         get_session/3,
         store_session/2]).

-include("ejabberd.hrl").
-include("logger.hrl").
-include("jlib.hrl").
-include("hb_session.hrl").

-record(state, {host = <<"">>,
                server_host = <<"">>,
                sid = <<"">>,
                jid = <<"">>,
                name = <<"">>,
                type = groupchat, %% chat | groupchat
                members = dict:new(),
                timer = null,
                data = null}).

-define(SESSION_TTL, 300000). %% 5 minutes

%%%===================================================================
%%% API
%%%===================================================================
start(Host, ServerHost, SessionId, Creator) ->
    hb_session_sup:start_child([Host, ServerHost, SessionId, Creator]).

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Host, ServerHost, SessionId, Creator) ->
    gen_fsm:start_link(?MODULE, [Host, ServerHost, SessionId, Creator], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Host, ServerHost, SessionId]) ->
    init([Host, ServerHost, SessionId, undefined]);
init([Host, ServerHost, SessionId, Creator]) ->
    timer:start(),
    SessionJID = jlib:string_to_jid(str:join([SessionId, Host], <<"@">>)),
    case get_session(Host, ServerHost, SessionId) of
        [#hb_session{} = SessionData] ->
            State = #state{host = Host,
                           server_host = ServerHost,
                           sid = SessionId,
                           jid = SessionJID,
                           type = SessionData#hb_session.type,
                           data = SessionData},
            NewState = reset_timer(State),
            case {SessionData#hb_session.type, Creator} of
                {groupchat, #jid{}} -> hb_session_log:create_user_session(SessionJID, [Creator], true);
                {chat, _} ->
                    Members = get_notify_members(State, message),
                    hb_session_log:create_user_session_for_chat(SessionJID, Members, Creator);
                _ -> ok
            end,
            mnesia:dirty_write(#hb_online_session{sid_host = {SessionId, ServerHost},
                                                  pid = self()}),
            ?INFO_MSG("session ~p started.", [NewState#state.jid]),
            {ok, normal_state, NewState};
        _ ->
            {stop, <<"hb_session not found">>}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec normal_state(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
normal_state({route, {From, To, #xmlel{name = Operation} = Packet}}, State) ->
    FromJID = jlib:jid_to_string(jlib:jid_remove_resource(From)),
    #hb_session{members = Members} = State#state.data,
    case dict:find(FromJID, Members) of
        {ok, _} ->
            case Operation of
                <<"iq">> ->
                    IQ = jlib:iq_query_info(Packet),
                    case process_iq(From, To, IQ, State) of
                        {ok, NewState} -> ok;
                        {error, NewState, #xmlel{} = Err} ->
                            ReplyIQ = jlib:make_error_reply(Packet, Err),
                            ejabberd_router:route(To, From, ReplyIQ);
                        _ ->
                            NewState = State
                    end;
                <<"message">> ->
                    case process_message(From, To, Packet, State) of
                        {ok, NewState} -> ok;
                        {error, NewState, #xmlel{} = Err} ->
                            ReplyIQ = jlib:make_error_reply(Packet, Err),
                            ejabberd_router:route(To, From, ReplyIQ);
                        _ ->
                            NewState = State
                    end;
                _ ->
                    NewState = State
            end;
        _ ->
            case Operation of
                <<"iq">> ->
                    IQ = #iq{sub_el = SubEl} = jlib:iq_query_info(Packet),
                    case {SubEl#xmlel.name, State#state.type} of
                        {SubOp, groupchat} when SubOp == <<"remove">> orelse SubOp == <<"read">> ->
                            case process_iq(From, To, IQ, State) of
                                {ok, NewState} -> ok;
                                {error, NewState, #xmlel{} = Err} ->
                                    ReplyIQ = jlib:make_error_reply(Packet, Err),
                                    ejabberd_router:route(To, From, ReplyIQ);
                                _ ->
                                    NewState = State
                            end;
                        {<<"leave">>, groupchat} ->
                            ReplyPacket = jlib:make_result_iq_reply((jlib:iq_to_xml(IQ))#xmlel{children = [SubEl]}),
                            ejabberd_router:route(To, From, ReplyPacket),
                            NewState = State;
                        _ ->
                            ReplyIQ = jlib:make_error_reply(Packet, ?ERR_NOT_ALLOWED),
                            ejabberd_router:route(To, From, ReplyIQ),
                            NewState = State
                    end;
                _ ->
                    ReplyIQ = jlib:make_error_reply(Packet, ?ERR_NOT_ALLOWED),
                    ejabberd_router:route(To, From, ReplyIQ),
                    NewState = State
            end
    end,
    NewState1 = reset_timer(NewState),
    {next_state, normal_state, NewState1};
normal_state({shutdown, Reason}, State) ->
    {stop, Reason, State};
normal_state(_Event, State) ->
    {next_state, normal_state, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec normal_state(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
normal_state(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, normal_state, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({shutdown, timeout}, _StateName, State) ->
    ?INFO_MSG("session ~p timeout, closed.", [State#state.jid]),
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #state{sid = SessionId, server_host = ServerHost}) ->
    remove_online_session(SessionId, ServerHost, self()),
    ok.

remove_online_session(SessionId, ServerHost, Pid) ->
    F = fun() ->
        mnesia:delete_object(#hb_online_session{sid_host = {SessionId, ServerHost}, pid = Pid})
    end,
    mnesia:async_dirty(F).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% --------------------------------------------------------------------
%% @doc
%%   处理iq
%% @spec process_iq(From, To, #iq{} = IQ, #state{} = State) ->
%%          {ok, NewState} | {error, NewState, #xmlel{} = Err}
%% @end
%% --------------------------------------------------------------------
process_iq(From, To, #iq{type = set, sub_el = SubEl} = IQ, #state{server_host = ServerHost, type = SessionType, data = SessionData} = State) ->
    #xmlel{name = Operation, attrs = Attrs} = SubEl,
    ReplyPacket = jlib:make_result_iq_reply((jlib:iq_to_xml(IQ))#xmlel{children = [SubEl]}),

    case {SessionType, Operation} of
        {groupchat, <<"invite">>} ->
            ItemJIDs = [ xml:get_attr_s(<<"jid">>, El#xmlel.attrs) || El <- SubEl#xmlel.children, El#xmlel.name == <<"item">> ],
            Members = SessionData#hb_session.members,
            {ok, Members1, AddedMembers} = add_jid_to_member_dict(From, ItemJIDs, Members),

            AddedItemsXML = [ #xmlel{name = <<"item">>, attrs = [{<<"jid">>, JID}]} || JID <- AddedMembers ],
            InviteXML = #xmlel{name = <<"x">>,
                               attrs = [{<<"xmlns">>, ?NS_HB_SESSION},
                                        {<<"operator">>, jlib:jid_to_string(jlib:jid_remove_resource(From))},
                                        {<<"operation">>, Operation}],
                               children = AddedItemsXML},
            BroadcastPacket = make_headline_broadcast(To, InviteXML),

            NewSessionData = SessionData#hb_session{members = Members1,
                                                    changed = now(),
                                                    last_message = BroadcastPacket},

            if length(AddedMembers) > 0 ->
                    case store_session(ServerHost, NewSessionData) of
                        ok ->
                            hb_session_log:create_user_session(To, [jlib:string_to_jid(R) || R <- AddedMembers]),

                            BroadcastJIDs = get_notify_members(State, iq),

                            %% reply to operator
                            ReplySubEl = SubEl#xmlel{children = AddedItemsXML},
                            ejabberd_router:route(To, From, ReplyPacket#xmlel{children = [ReplySubEl]}),

                            %% broadcast to members
                            broadcast(To, BroadcastJIDs, BroadcastPacket),
                            hb_message_log:store_chat_message(From, To, BroadcastPacket),

                            NewState = State#state{data = NewSessionData},
                            {ok, NewState};
                        error ->
                            {error, State, ?ERR_INTERNAL_SERVER_ERROR}
                    end;
                true ->
                    ReplySubEl = SubEl#xmlel{children = []},
                    ejabberd_router:route(To, From, ReplyPacket#xmlel{children = [ReplySubEl]}),
                    {ok, State}
            end;
        {groupchat, <<"kick">>} ->
            ItemJIDs = [ xml:get_attr_s(<<"jid">>, El#xmlel.attrs) || El <- SubEl#xmlel.children, El#xmlel.name == <<"item">> ],
            Members = SessionData#hb_session.members,
            {ok, Members1, RemovedMembers} = remove_jid_from_member_dict(ItemJIDs, Members),

            RemovedItemsXML = [ #xmlel{name = <<"item">>, attrs = [{<<"jid">>, JID}]} || JID <- RemovedMembers ],
            KickXML = #xmlel{name = <<"x">>,
                               attrs = [{<<"xmlns">>, ?NS_HB_SESSION},
                                        {<<"operator">>, jlib:jid_to_string(jlib:jid_remove_resource(From))},
                                        {<<"operation">>, Operation}],
                               children = RemovedItemsXML},
            BroadcastPacket = make_headline_broadcast(To, KickXML),

            NewSessionData = SessionData#hb_session{members = Members1,
                                                    changed = now(),
                                                    last_message = BroadcastPacket},

            if length(RemovedMembers) > 0 ->
                    case store_session(ServerHost, NewSessionData) of
                        ok ->
                            BroadcastJIDs = get_notify_members(State, iq),

                            %% reply to operator
                            ReplySubEl = SubEl#xmlel{children = RemovedItemsXML},
                            ejabberd_router:route(To, From, ReplyPacket#xmlel{children = [ReplySubEl]}),

                            %% broadcast to members
                            broadcast(To, BroadcastJIDs, BroadcastPacket),
                            hb_message_log:store_chat_message(From, To, BroadcastPacket),

                            hb_session_log:delete_user_session(To,
                                                               [jlib:string_to_jid(R) || R <- RemovedMembers],
                                                               get_session_snapshot_data(NewSessionData)),

                            NewState = State#state{data = NewSessionData},
                            {ok, NewState};
                        error ->
                            {error, State, ?ERR_INTERNAL_SERVER_ERROR}
                    end;
                true ->
                    ReplySubEl = SubEl#xmlel{children = []},
                    ejabberd_router:route(To, From, ReplyPacket#xmlel{children = [ReplySubEl]}),
                    {ok, State}
            end;
        {groupchat, <<"leave">>} ->
            JID = jlib:jid_to_string(jlib:jid_remove_resource(From)),
            Members = SessionData#hb_session.members,
            {ok, Members1, RemovedMembers} = remove_jid_from_member_dict([JID], Members),

            RemovedItemsXML = [ #xmlel{name = <<"item">>, attrs = [{<<"jid">>, RemovedJID}]} || RemovedJID <- RemovedMembers ],
            LeaveXML = #xmlel{name = <<"x">>,
                               attrs = [{<<"xmlns">>, ?NS_HB_SESSION},
                                        {<<"operator">>, jlib:jid_to_string(jlib:jid_remove_resource(From))},
                                        {<<"operation">>, Operation}],
                               children = RemovedItemsXML},
            BroadcastPacket = make_headline_broadcast(To, LeaveXML),

            NewSessionData = SessionData#hb_session{members = Members1,
                                                    changed = now(),
                                                    last_message = BroadcastPacket},

            case store_session(ServerHost, NewSessionData) of
                ok ->
                    ejabberd_router:route(To, From, ReplyPacket),

                    BroadcastJIDs = get_notify_members(State, iq),
                    broadcast(To, BroadcastJIDs, BroadcastPacket),
                    hb_message_log:store_chat_message(From, To, BroadcastPacket),

                    hb_session_log:delete_user_session(To,
                                                       [jlib:string_to_jid(R) || R <- RemovedMembers],
                                                       get_session_snapshot_data(NewSessionData)),

                    NewState = State#state{data = NewSessionData},
                    {ok, NewState};
                error ->
                    {error, State, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        {groupchat, <<"rename">>} ->
            OriginalName = SessionData#hb_session.name,
            case xml:get_attr_s(<<"name">>, Attrs) of
                <<>> ->
                    {error, State, ?ERR_INTERNAL_SERVER_ERROR};
                OriginalName ->
                    {ok, State};
                Name ->
                    BroadcastJIDs = get_notify_members(State, iq),

                    Attrs1 = lists:keydelete(<<"xmlns">>, 1, Attrs),
                    Attrs2 = [{<<"xmlns">>, ?NS_HB_SESSION_CONFIG},
                              {<<"operator">>, jlib:jid_to_string(From)}] ++ Attrs1,

                    BroadcastPacket = make_result_iq_reply(To, SubEl#xmlel{attrs = Attrs2}),

                    %% 添加一个修改群名的消息
                    RenameXML = #xmlel{name = <<"x">>,
                                               attrs = [{<<"xmlns">>, ?NS_HB_SESSION},
                                                        {<<"operator">>, jlib:jid_to_string(jlib:jid_remove_resource(From))},
                                                        {<<"operation">>, Operation},
                                                        {<<"value">>, Name}],
                                               children = []},
                    NotifyPacket = make_headline_broadcast(To, RenameXML),

                    NewSessionData = SessionData#hb_session{name = Name,
                                                            changed = now(),
                                                            last_message = NotifyPacket},
                    case store_session(ServerHost, NewSessionData) of
                        ok ->
                            ejabberd_router:route(To, From, ReplyPacket),
                            broadcast(To, BroadcastJIDs, BroadcastPacket),

                            hb_message_log:store_chat_message(From, To, NotifyPacket),
                            broadcast(To, BroadcastJIDs, NotifyPacket),

                            NewState = State#state{name = Name, data = NewSessionData},
                            {ok, NewState};
                        error ->
                            {error, State, ?ERR_INTERNAL_SERVER_ERROR}
                    end
            end;
        {groupchat, <<"notify">>} ->
            {Value, Notify} = case xml:get_attr_s(<<"value">>, Attrs) of
                <<"0">> -> {<<"0">>, false};
                <<"1">> -> {<<"1">>, true};
                _ -> {<<"1">>, true}
            end,
            case hb_session_log:store_session_flag(From, To, notify, Value) of
                ok ->
                    ejabberd_router:route(To, From, ReplyPacket),

                    NewSessionData = change_user_config(From, {<<"notify">>, Notify}, SessionData),

                    ReplyIQ = make_result_iq_reply(To, SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_HB_SESSION_STATUS}]}),
                    broadcast(To, From, ReplyIQ),
                    {ok, State#state{data = NewSessionData}};
                _ ->
                    {error, State, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        {_, <<"remove">>} ->
            case hb_session_log:store_session_flag(From, To, isdeleted, <<"1">>) of
                ok ->
                    ejabberd_router:route(To, From, ReplyPacket),

                    ReplyIQ = make_result_iq_reply(To, SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_HB_SESSION_CONFIG}]}),
                    broadcast(To, From, ReplyIQ),
                    {ok, State};
                _ ->
                    {error, State, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        {_, <<"read">>} ->
            case hb_session_log:store_read_flag(From, To) of
                ok ->
                    ejabberd_router:route(To, From, ReplyPacket),

                    ReplyIQ = make_result_iq_reply(To, SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_HB_SESSION_STATUS}]}),
                    broadcast(To, From, ReplyIQ),
                    {ok, State};
                _ ->
                    {error, State, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        _ ->
            {error, State, ?ERR_FEATURE_NOT_IMPLEMENTED}
    end;
process_iq(From, To, #iq{type = get, sub_el = SubEl} = IQ, #state{data = SessionData} = State) ->
    #xmlel{name = Operation, attrs = Attrs} = SubEl,
    NS = xml:get_attr_s(<<"xmlns">>, Attrs),
    case {Operation, NS} of
        {<<"query">>, ?NS_DISCO_ITEMS} ->
            Items = [ #xmlel{name = <<"item">>, attrs = [{<<"jid">>, JID}]} || {JID, _} <- dict:to_list(SessionData#hb_session.members) ],
            ReplyPacket = jlib:iq_to_xml(IQ#iq{sub_el = [SubEl#xmlel{children = Items}]}),
            ReplyIQ = jlib:make_result_iq_reply(ReplyPacket),
            ejabberd_router:route(To, From, ReplyIQ),
            {ok, State};
        {<<"query">>, ?NS_DISCO_INFO} ->
            SessionInfos = [{<<"name">>, (State#state.data)#hb_session.name},
                            {<<"type">>, atom_to_binary(State#state.type, latin1)}],

            UserSessionInfos = case hb_session_log:get_user_session_info(From, To) of
                {ok, {Unread, Notify}} ->
                    [{<<"unread">>, Unread},
                     {<<"notify">>, Notify}];
                Err ->
                    ?ERROR_MSG("error while get user sesssion info: ~p", [Err]),
                    []
            end,

            Infos = SessionInfos ++ UserSessionInfos,
            Items = lists:map(fun({Name, Value}) ->
                            ValueEl = #xmlel{name = <<"value">>,
                                             children = [{xmlcdata, Value}]},
                            #xmlel{name = <<"field">>,
                                   attrs = [{<<"var">>, Name}],
                                   children = [ValueEl]}
                    end, Infos),
            ConfigEl = #xmlel{name = <<"x">>,
                              attrs = [{<<"xmlns">>, ?NS_XDATA}],
                              children = Items},

            ReplyPacket = jlib:iq_to_xml(IQ#iq{sub_el = [SubEl#xmlel{children = [ConfigEl]}]}),
            ReplyIQ = jlib:make_result_iq_reply(ReplyPacket),
            ejabberd_router:route(To, From, ReplyIQ),
            {ok, State};
        _ ->
            {error, State, ?ERR_FEATURE_NOT_IMPLEMENTED}
    end;
process_iq(_From, _To, _IQ, State) ->
    {error, State, ?ERR_FEATURE_NOT_IMPLEMENTED}.

make_result_iq_reply(From, SubEl) when is_list(SubEl) ->
    #xmlel{name = <<"iq">>,
           attrs = [{<<"from">>, jlib:jid_to_string(From)},
                    {<<"type">>, <<"result">>}],
           children = SubEl};
make_result_iq_reply(From, #xmlel{} = SubEl) ->
    make_result_iq_reply(From, [SubEl]).

make_headline_broadcast(From, SubEls) when is_list(SubEls) ->
    Type = <<"headline">>,
    MId = generate_message_id(From, Type),
    Delay = hblib:timestamp_to_xml(now(), From, <<>>),
    #xmlel{name = <<"message">>,
           attrs = [{<<"from">>, jlib:jid_to_string(From)},
                    {<<"type">>, Type},
                    {<<"mid">>, MId}],
           children = SubEls ++ [Delay]};
make_headline_broadcast(From, #xmlel{} = SubEl) ->
    make_headline_broadcast(From, [SubEl]).

get_session_snapshot_data(#hb_session{name = Name, members = Members, last_message = LastMessage}) ->
    {Name, Members, LastMessage}.

change_user_config(User, {Key, Value}, #hb_session{members = Members} = SessionData) ->
    JId = hblib:get_bare_jid(User),
    MemberConfig = lists:keystore(Key, 1, dict:fetch(JId, Members), {Key, Value}),
    SessionData#hb_session{members = dict:store(JId, MemberConfig, Members)}.

%% --------------------------------------------------------------------
%% @doc
%%   将jid转换后插入到members列表
%% @spec spec add_jid_to_member_dict(Items, Members) ->
%%               {ok, NewMembers, AddedMembers}
%% @end
%% --------------------------------------------------------------------
add_jid_to_member_dict(Operator, Items, Members) ->
    add_jid_to_member_dict(Operator, Items, Members, []).

add_jid_to_member_dict(_Operator, [], Members, Acc) ->
    {ok, Members, Acc};
add_jid_to_member_dict(Operator, [Key | Tail], Members, Acc) ->
    %% todo: privacy check
    JID = jlib:jid_remove_resource(jlib:string_to_jid(Key)),
    case dict:find(Key, Members) of
        {ok, _} ->
            Acc1 = Acc,
            Members1 = Members;
        error ->
            Acc1 = [Key | Acc],
            Members1 = dict:append(Key, {<<"jid">>, JID}, Members)
    end,
    add_jid_to_member_dict(Operator, Tail, Members1, Acc1).

%% --------------------------------------------------------------------
%% @doc
%%   从members列表移除若干个jid
%% @spec spec remove_jid_from_member_dict(Items, Members) ->
%%               {ok, NewMembers, RemovedMembers}
%% @end
%% --------------------------------------------------------------------
remove_jid_from_member_dict(Items, Members) ->
    remove_jid_from_member_dict(Items, Members, []).

remove_jid_from_member_dict([], Members, Acc) ->
    {ok, Members, Acc};
remove_jid_from_member_dict([Key | Tail], Members, Acc) ->
    case dict:find(Key, Members) of
        {ok, _} ->
            Acc1 = [Key | Acc],
            Members1 = dict:erase(Key, Members);
        error ->
            Acc1 = Acc,
            Members1 = Members
    end,
    remove_jid_from_member_dict(Tail, Members1, Acc1).

%% --------------------------------------------------------------------
%% @doc
%%   处理用户发送消息
%% @spec process_message(From, To, Packet, State) ->
%%          {ok, NewState} |
%%          {error, NewState, Err :: xmlel()}
%% @end
%% --------------------------------------------------------------------
process_message(From, To, #xmlel{attrs = Attrs, children = SubEls} = Packet, #state{server_host = ServerHost} = State) ->
    BroadcastJIDs = get_notify_members(State, message),
    Type = xml:get_attr_s(<<"type">>, Attrs),
    ID = xml:get_attr_s(<<"id">>, Attrs),
    User = jlib:jid_remove_resource(From),
    SUser = jlib:jid_to_string(User),

    if Type == <<"chat">> orelse Type == <<"groupchat">> ->
            SessionData = State#state.data,
            PrevMId = case SessionData#hb_session.last_message of
                #xmlel{attrs = PrevAttrs} ->
                    xml:get_attr_s(<<"mid">>, PrevAttrs);
                _ -> <<>>
            end,

            MessageId = generate_message_id(From, To),
            Attrs1 = [{<<"from">>, jlib:jid_to_string(To)},
                      {<<"user">>, SUser},
                      {<<"type">>, atom_to_binary(State#state.type, latin1)},
                      {<<"mid">>, MessageId},
                      {<<"prevmid">>, PrevMId}],
            Delay = hblib:timestamp_to_xml(now(), From, <<>>),
            Packet1 = xml:remove_subtags(Packet, <<"delay">>, {<<"xmlns">>, ?NS_DELAY}),
            Message = Packet1#xmlel{attrs = Attrs1, children = SubEls ++ [Delay]},

            %% update session time
            NewSessionData = SessionData#hb_session{last_message = Message,
                                                    changed = now()},
            NewState = State#state{data = NewSessionData},
            case store_session(ServerHost, NewSessionData) of
                ok ->
                    hb_message_log:store_chat_message(From, To, Message),
                    hb_session_log:update_user_session(To, BroadcastJIDs, User),

                    BroadcastMessage = case ID of
                        <<>> -> Message;
                        _ ->
                            Attrs2 = [{<<"id">>, ID} | Attrs1],
                            Message#xmlel{attrs = Attrs2}
                    end,
                    broadcast(To, BroadcastJIDs, BroadcastMessage, get_blocked_members(NewState)),
                    {ok, NewState};
                error ->
                    {error, State, ?ERR_INTERNAL_SERVER_ERROR}
            end;
       true ->
            {error, State, ?ERR_NOT_ALLOWED}
    end.


generate_message_id(From, To) ->
    p1_sha:sha(term_to_binary([From,
                               To,
                               now(),
                               randoms:get_string()])).

%% --------------------------------------------------------------------
%% @doc
%%   获取接收通知的用户列表
%% @spec get_notify_members(State, iq) -> list()
%%       get_notify_members(State, message) -> list()
%% @end
%% --------------------------------------------------------------------
get_notify_members(#state{server_host = ServerHost, jid = SessionJID}, iq) ->
    hb_session_log:get_exist_members(ServerHost, SessionJID);
get_notify_members(#state{data = #hb_session{members = Members}}, message) ->
    lists:map(fun({_, Value}) ->
                {value, {_, JId}, _} = lists:keytake(<<"jid">>, 1, Value),
                JId
        end, dict:to_list(Members)).

get_blocked_members(#state{data = #hb_session{members = Members}}) ->
    lists:map(fun({_, Value}) ->
                {value, {_, JId}, _} = lists:keytake(<<"jid">>, 1, Value),
                JId
        end, lists:filter(fun({_, Value}) ->
                    case lists:keytake(<<"notify">>, 1, Value) of
                        {value, {_, Notify}, _} ->
                            case Notify of
                                true -> false;
                                false -> true
                            end;
                        false -> false
                    end
        end, dict:to_list(Members))).

%% --------------------------------------------------------------------
%% @doc
%%   向用户的所有客户端广播消息
%% @spec broadcast(From, To, Packet) -> ok
%% @end
%% --------------------------------------------------------------------
broadcast(From, To, Packet) ->
    broadcast(From, To, Packet, [], undefined).

broadcast(From, To, Packet, BlockMembers) ->
    broadcast(From, To, Packet, BlockMembers, fun offline_message_push/3).

broadcast(From, Members, Packet, BlockMembers, OfflineHandle) when is_list(Members) ->
    lists:foreach(fun(To) ->
                broadcast(From, To, Packet, BlockMembers, OfflineHandle)
        end, Members);
broadcast(From, To, Packet, BlockMembers, OfflineHandle) ->
    Resources = ejabberd_sm:get_user_resources(To#jid.user, To#jid.server),
    OfflineCallback = case lists:member(To, BlockMembers) of
        true ->
            ?INFO_MSG("user ~p blocked. offline push abort", [To]),
            abort;
        false -> OfflineHandle
    end,
    route_packet_to_user(From, To, Packet, Resources, OfflineCallback).

route_packet_to_user(From, To, Packet, [], OfflineHandle) when is_function(OfflineHandle) ->
    OfflineHandle(From, To, Packet);
route_packet_to_user(_From, _To, _Packet, [], _) ->
    ok;
route_packet_to_user(From, To, Packet, Resources, _) ->
    lists:foreach(fun(R) ->
                  ejabberd_router:route(From, jlib:jid_replace_resource(To, R), Packet)
          end, Resources).

offline_message_push(From, To, Packet) ->
    ejabberd_hooks:run(hb_offline_message_push, To#jid.lserver, [From, To, Packet]).

%% --------------------------------------------------------------------
%% @doc
%%   检查指定的会话是否存在
%% @spec session_exists(Host, SessionId) -> true | false
%% @end
%% --------------------------------------------------------------------
session_exists(Host, ServerHost, SessionId) ->
    DBType = gen_mod:db_type(ServerHost, hb_session_server),
    session_exists(Host, ServerHost, SessionId, DBType).

session_exists(Host, ServerHost, SessionId, mnesia) ->
    case length(get_session(Host, ServerHost, SessionId, mnesia)) of
        0 -> false;
        _ -> true
    end;
session_exists(Host, ServerHost, SessionId, odbc) ->
    Sid = hblib:escape(str:join([SessionId, Host], <<"@">>)),
    Query = [<<"SELECT sid ">>, <<"From hb_session Where ">>, <<" sid = '">>, Sid, <<"';">>],
    case catch ejabberd_odbc:sql_query(ServerHost, Query) of
        {selected, _, []} -> false;
        {selected, _, _Rs} -> true;
        _ -> false
    end.

%% --------------------------------------------------------------------
%% @doc
%%   取指定会话的详情
%% @spec get_session(Host, SessionId) -> [#hb_session{}] | []
%% @end
%% --------------------------------------------------------------------
get_session(Host, ServerHost, SessionId) ->
    DBType = gen_mod:db_type(ServerHost, hb_session_server),
    get_session(Host, ServerHost, SessionId, DBType).

get_session(Host, _ServerHost, SessionId, mnesia) ->
    case mnesia:dirty_read(hb_session, {SessionId, Host}) of
        [#hb_session{} = SessionData] -> [SessionData];
        _ -> []
    end;
get_session(Host, ServerHost, SessionId, odbc) ->
    Sid = hblib:escape(str:join([SessionId, Host], <<"@">>)),
    F = fun() ->
        Query = [<<"SELECT sid, name, type, members, last_message, changed ">>,
                 <<"From hb_session Where ">>,
                 <<" sid = '">>, Sid, <<"';">>],
        UserLogQuery = [<<"SELECT uid, notify FROM hb_user_session WHERE sid = '">>, Sid, <<"' AND `isquit` = 0;">>],
        UserSessionLogs = case ejabberd_odbc:sql_query_t(UserLogQuery) of
            {selected, _, Rs} -> [{UId, case Notify of <<"0">> -> false; _ -> true end} || [UId, Notify] <- Rs];
            _ -> []
        end,
        case ejabberd_odbc:sql_query_t(Query) of
            {selected, _, [[_, Name, Type, Members, LastMessage, Changed]]} ->
                MessageEl = case xml_stream:parse_element(LastMessage) of
                    {error, _} -> null;
                    El -> El
                end,
                MemberDict = dict:from_list(lists:map(fun(Key) ->
                                        JId = jlib:string_to_jid(Key),
                                        Notify = case lists:keytake(Key, 1, UserSessionLogs) of
                                            {value, {Key, Value}, _} -> Value;
                                            _ -> true
                                        end,
                                        ?INFO_MSG("in session ~p: user ~p notify ~p~n~n", [Sid, JId, Notify]),
                                        {Key, [{<<"jid">>, JId}, {<<"notify">>, Notify}]}
                                end, str:tokens(Members, <<",">>))),
                [#hb_session{sid_host = {SessionId, Host},
                             name = Name,
                             type = case Type of <<"chat">> -> chat; <<"groupchat">> -> groupchat end,
                             members = MemberDict,
                             last_message = MessageEl,
                             changed = hblib:integer_to_timestamp(Changed)}];
            _ -> []
        end
    end,
    case catch ejabberd_odbc:sql_transaction(ServerHost, F) of
        {atomic, Rs} ->
            Rs;
        {aborted, Err} ->
            ?ERROR_MSG("error while get session ~p", [Err]),
            []
    end.

%% --------------------------------------------------------------------
%% @doc
%%   存储会话信息
%% @spec store_session(#hb_session{} = SessionData) -> ok | error
%% @end
%% --------------------------------------------------------------------
store_session(ServerHost, SessionData) ->
    DBType = gen_mod:db_type(ServerHost, hb_session_server),
    store_session(ServerHost, SessionData, DBType).

store_session(_ServerHost, SessionData, mnesia) ->
    F = fun() ->
            mnesia:write(SessionData)
    end,
    case mnesia:transaction(F) of
        {atomic, ok} -> ok;
        _ -> error
    end;
store_session(ServerHost, SessionData, odbc) ->
    {SessionId, Host} = SessionData#hb_session.sid_host,
    Sid = hblib:escape(str:join([SessionId, Host], <<"@">>)),
    Name = hblib:escape(SessionData#hb_session.name),
    Members = hblib:escape(str:join(dict:fetch_keys(SessionData#hb_session.members), <<",">>)),
    Type = hblib:escape(SessionData#hb_session.type),

    MessageEl = SessionData#hb_session.last_message,
    LastMessage = case MessageEl of
        null -> <<"">>;
        #xmlel{} -> hblib:escape(xml:element_to_binary(MessageEl))
    end,

    Changed = case SessionData#hb_session.changed of
        null -> <<"">>;
        TimeStamp ->
            DateTime = hblib:timestamp_to_integer(TimeStamp),
            hblib:escape(DateTime)
    end,

    Query = [<<"REPLACE INTO hb_session (sid, name, type, members, last_message, changed) VALUES (">>,
             <<"'">>, Sid, <<"', '">>, Name, <<"', '">>, Type, <<"', '">>, Members, <<"', '">>, LastMessage,
             <<"', '">>, Changed, <<"');">>],
    case catch ejabberd_odbc:sql_query(ServerHost, Query) of
        {updated, _} -> ok;
        _ -> error
    end.

%%====================================================================
%% Timer handling
%%====================================================================
reset_timer(State) ->
    if is_reference(State#state.timer) ->
            timer:cancel(State#state.timer);
       true -> ok
    end,
    Timer = timer:send_after(?SESSION_TTL, self(), {shutdown, timeout}),
    State#state{timer = Timer}.
