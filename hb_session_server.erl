-module(hb_session_server).

-behaviour(gen_server).

-behaviour(gen_mod).

%% API
-export([start_link/2,
         start/2,
         stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ejabberd.hrl").
-include("logger.hrl").
-include("jlib.hrl").
-include("hb_session.hrl").

-record(state, {host = <<"">>,
                server_host = <<"">>}).

-define(PROCNAME, hb_session_server).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:start_link({local, Proc}, ?MODULE,
			  [Host, Opts], []).

start(Host, Opts) ->
    start_supervisor(Host),
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    ChildSpec = {Proc, {?MODULE, start_link, [Host, Opts]},
		 permanent, 1000, worker, [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec).

stop(Host) ->
    Sessions = shutdown_sessions(Host),
    stop_supervisor(Host),
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:call(Proc, stop),
    supervisor:delete_child(ejabberd_sup, Proc),
    {wait, Sessions}.

shutdown_sessions(Host) ->
    Rooms = mnesia:dirty_select(hb_online_session,
				[{#hb_online_session{sid_host = '$1',
						   pid = '$2'},
				  [{'==', {element, 2, '$1'}, Host}],
				  ['$2']}]),
    [Pid ! shutdown || Pid <- Rooms],
    Rooms.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Host, Opts]) ->
    MyHost = gen_mod:get_opt_host(Host, Opts,
				  <<"session.@HOST@">>),

    %% 创建会话存储表
    Fields = record_info(fields, hb_session),
    try mnesia:table_info(hb_session, attributes) of
	    Fields -> ok;
	    _ -> mnesia:delete_table(hb_session)  %% recreate..
    catch
        _:_Error -> ok  %%probably table don't exist
    end,
    mnesia:create_table(hb_session,
                        [{disc_copies, [node()]},
                         {attributes, record_info(fields, hb_session)}]),

    %% 创建在线会话临时存储
    mnesia:create_table(hb_online_session,
                        [{ram_copies, [node()]},
                         {attributes, record_info(fields, hb_online_session)}]),
    mnesia:add_table_copy(hb_online_session, node(), ram_copies),
    clean_table_from_bad_node(node(), MyHost),

    %% 注册路由 将发送到MyHost的包转到此server
    ejabberd_router:register_route(MyHost),
    {ok, #state{host = MyHost,
                server_host = Host}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({route, From, To, Packet}, #state{host = Host, server_host = ServerHost} = State) ->
    if length(Packet#xmlel.children) > 0 ->
            do_route(Host, ServerHost, From, To, Packet);
        true ->
            ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    Host = State#state.server_host,
    Sessions = shutdown_sessions(Host),
    ejabberd_router:unregister_route(Host),
    {wait, Sessions}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_supervisor(Host) ->
    Proc = gen_mod:get_module_proc(Host,
				   hb_session_sup),
    ChildSpec = {Proc,
                 {hb_session_sup, start_link, []},
		         permanent, infinity, supervisor, [hb_session_sup]},
    supervisor:start_child(ejabberd_sup, ChildSpec).

stop_supervisor(Host) ->
    Proc = gen_mod:get_module_proc(Host,
				   hb_session_sup),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).

do_route(Host, ServerHost, From, To, #xmlel{name = <<"iq">>} = Packet) ->
    Type = xml:get_attr_s(<<"type">>, Packet#xmlel.attrs),
    [SubEl | _] = Packet#xmlel.children,
    case {SubEl, Type} of

        {#xmlel{name = <<"create">>, attrs = Attrs}, <<"set">>} ->
            SessionType = xml:get_attr_s(<<"type">>, Attrs),
            NS = xml:get_attr_s(<<"xmlns">>, Attrs),
            Name = xml:get_attr_s(<<"name">>, Attrs),

            ReplyIQ = case {SessionType, NS} of
                {<<"chat">>, ?NS_HB_SESSION} ->
                    case xml:get_attr_s(<<"with">>, Attrs) of
                        <<>> ->
                            jlib:make_error_reply(Packet, ?ERR_BAD_REQUEST);
                        With ->
                            WithJID = jlib:jid_remove_resource(jlib:string_to_jid(With)),
                            %% todo: privacy check
                            case create_session(Host, ServerHost, chat, [From, WithJID]) of
                                {ok, SessionId} ->
                                    ReplyPacket = jlib:make_result_iq_reply(Packet),
                                    ReplySubEl = [SubEl#xmlel{attrs = [{<<"sid">>, SessionId} | Attrs]}],
                                    ReplyPacket#xmlel{children = ReplySubEl};
                                _ ->
                                    jlib:make_error_reply(Packet, ?ERR_INTERNAL_SERVER_ERROR)
                            end
                    end;
                {<<"groupchat">>, ?NS_HB_SESSION} ->
                    case create_session(Host, ServerHost, groupchat, [From], Name) of
                        {ok, SessionId} ->
                            ReplyPacket = jlib:make_result_iq_reply(Packet),
                            ReplySubEl = [SubEl#xmlel{attrs = [{<<"sid">>, SessionId} | Attrs]}],
                            ReplyPacket#xmlel{children = ReplySubEl};
                        _ ->
                            jlib:make_error_reply(Packet, ?ERR_INTERNAL_SERVER_ERROR)
                    end;
                _ ->
                    jlib:make_error_reply(Packet, ?ERR_FEATURE_NOT_IMPLEMENTED)
            end,
            ejabberd_router:route(To, From, ReplyIQ);

        {#xmlel{attrs = Attrs}, _} ->
            NS = xml:get_attr_s(<<"xmlns">>, Attrs),
            if  NS == <<"">> -> ok;
                true -> load_and_route(Host, ServerHost, From, To, Packet)
            end;

        _ ->
            ReplyIQ = jlib:make_error_reply(Packet, ?ERR_UNEXPECTED_REQUEST_CANCEL),
            ejabberd_router:route(To, From, ReplyIQ)
    end;
do_route(Host, ServerHost, From, To, #xmlel{name = <<"message">>} = Packet) ->
    load_and_route(Host, ServerHost, From, To, Packet);
do_route(_Host, _ServerHost, _From, _To, _Packet) ->
    ok.

%% 加载并将packet转发到会话进程
load_and_route(Host, ServerHost, From, To, Packet) ->
    #jid{luser = SessionId} = To,
    RouteData = {From, To, Packet},
    case mnesia:dirty_read(hb_online_session, {SessionId, ServerHost}) of
        [#hb_online_session{pid = Pid}] ->
            route_to_session(Pid, RouteData);
        [] ->
            case hb_session:session_exists(Host, ServerHost, SessionId) of
                false ->
                    ?ERROR_MSG("~n~nsession not found: ~p~n~n", [SessionId]),
                    ReplyPacket = jlib:make_error_reply(Packet, ?ERR_ITEM_NOT_FOUND),
                    ejabberd_router:route(To, From, ReplyPacket);
                true ->
                    case start_session(Host, ServerHost, SessionId) of
                        {ok, Pid} ->
                            route_to_session(Pid, RouteData);
                        {error, Err} ->
                            ?ERROR_MSG("start session error: ~p", [Err]),
                            ReplyPacket = jlib:make_error_reply(Packet, ?ERR_INTERNAL_SERVER_ERROR),
                            ejabberd_router:route(To, From, ReplyPacket)
                    end
            end
    end.

route_to_session(Pid, RouteData) ->
    gen_fsm:send_event(Pid, {route, RouteData}).

create_session(Host, ServerHost, Type, [Operator | _] = Members) ->
    create_session(Host, ServerHost, Type, [Operator | _] = Members, <<"">>).

create_session(Host, ServerHost, Type, [Operator | _] = Members, Name) ->
    SessionId = generate_session_id(Host, Type, Members),
    Session = #hb_session{sid_host = {SessionId, Host}, type = Type, name = Name},

    InitMembers = lists:foldl(fun(Member, Dict) ->
                                  JID = jlib:jid_remove_resource(Member),
                                  Key = jlib:jid_to_string(JID),
                                  dict:append(Key, {<<"jid">>, JID}, Dict)
                              end, Session#hb_session.members, Members),
    SessionData = Session#hb_session{members = InitMembers},

    case hb_session:session_exists(Host, ServerHost, SessionId) of
        true ->
            {ok, SessionId};
        false ->
            case hb_session:store_session(ServerHost, SessionData) of
                ok ->
                    case start_session(Host, ServerHost, SessionId, jlib:jid_remove_resource(Operator)) of
                        {ok, _Pid} ->
                            {ok, SessionId};
                        {error, Err} ->
                            ?ERROR_MSG("create session error ~p", [Err]),
                            error
                    end;
                error ->
                    error
            end
    end.

generate_session_id(Host, groupchat, Members) ->
    p1_sha:sha(term_to_binary([Host,
                               Members,
                               now(),
                               randoms:get_string()]));
generate_session_id(Host, chat, Members) ->
    Key = lists:usort(
            lists:map(fun (JID) ->
                        jlib:jid_to_string(jlib:jid_remove_resource(JID))
                end, Members)),
    p1_sha:sha(term_to_binary([Host, Key])).

start_session(Host, ServerHost, SessionId) ->
    start_session(Host, ServerHost, SessionId, undefined).

start_session(Host, ServerHost, SessionId, Creator) ->
    case hb_session:start(Host, ServerHost, SessionId, Creator) of
        {ok, Pid} ->
            mnesia:dirty_write(#hb_online_session{sid_host = {SessionId, ServerHost},
                                                  pid = Pid}),
            {ok, Pid};
        Err ->
            {error, Err}
    end.

clean_table_from_bad_node(Node) ->
    F = fun() ->
		Es = mnesia:select(
		       hb_online_session,
		       [{#hb_online_session{pid = '$1', _ = '_'},
			 [{'==', {node, '$1'}, Node}],
			 ['$_']}]),
		lists:foreach(fun(E) ->
				      mnesia:delete_object(E)
			      end, Es)
        end,
    mnesia:async_dirty(F).

clean_table_from_bad_node(Node, Host) ->
    F = fun() ->
		Es = mnesia:select(
		       hb_online_session,
		       [{#hb_online_session{pid = '$1',
					  sid_host = {'_', Host},
					  _ = '_'},
			 [{'==', {node, '$1'}, Node}],
			 ['$_']}]),
		lists:foreach(fun(E) ->
				      mnesia:delete_object(E)
			      end, Es)
        end,
    mnesia:async_dirty(F).
