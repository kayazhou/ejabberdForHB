-module(hb_apple_push).

-behavior(gen_mod).

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_mod callbacks
-export([start/2,
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
-include("amqp_client.hrl").

%% hooks
-export([handle_apple_push/3]).

-record(state, {host, channel, rabbit, rabbit_host, rabbit_queue}).

-record(hb_user_device, {imei = <<"">>,
                         uid_host = {<<"">>, <<"">>},
                         type = ios, %% ios | android | wp
                         token = <<"">>}).

-define(SUPERVISOR, ejabberd_sup).

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
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:start_link({local, Proc}, ?MODULE,
              [Host, Opts], []).

%%====================================================================
%% gen_mod callbacks
%%====================================================================
start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    ChildSpec = {Proc, {?MODULE, start_link, [Host, Opts]},
        permanent, 1000, worker, [?MODULE]},
    supervisor:start_child(?SUPERVISOR, ChildSpec).

stop(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Proc, stop),
    supervisor:delete_child(?SUPERVISOR, Proc).

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
    RabbitHost = gen_mod:get_opt(rabbit_host, Opts,
                                            fun(H) -> binary_to_list(H)
                                        end, "localhost"),
    RabbitUsername = gen_mod:get_opt(rabbit_username, Opts, fun(H) -> H end, <<"guest">>),
    RabbitPassword = gen_mod:get_opt(rabbit_password, Opts, fun(H) -> H end, <<"guest">>),
    RabbitVhost = gen_mod:get_opt(rabbit_vhost, Opts, fun(H) -> H end, <<"/">>),
    RabbitQueue = gen_mod:get_opt(rabbit_queue, Opts, fun(Q) -> Q end, <<"mobile_push_notification">>),

    ejabberd_hooks:add(hb_offline_message_push, Host, ?MODULE, handle_apple_push, 50),
    case catch amqp_connection:start(#amqp_params_network{host = RabbitHost,
                                                          username = RabbitUsername,
                                                          password = RabbitPassword,
                                                          virtual_host = RabbitVhost}) of
        {ok, Connection} ->
            {ok, Channel} = amqp_connection:open_channel(Connection),
            #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = RabbitQueue, durable = true}),
            ?INFO_MSG("~n~nrabbit connect ok: ~p, channel: ~p~n~n", [Connection, Channel]),
            {ok, #state{host = Host,
                        rabbit = Connection,
                        channel = Channel,
                        rabbit_host = RabbitHost,
                        rabbit_queue = RabbitQueue}};
        Err ->
            ?ERROR_MSG("rabbitmq connect error", []),
            {stop, {<<"rabbitmq connect error">>, Err}}
    end.

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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
handle_cast({push_message, {From, To, #xmlel{name = <<"message">>, attrs = Attrs} = Packet}}, State) ->
    #jid{luser = User, lserver = Host} = To,
    Es = mnesia:dirty_select(hb_user_device,
                             [{#hb_user_device{imei = '_', uid_host = '$1', type = '$2', _ = '_'},
                               [{'==', {element, 1, '$1'}, User},
                                {'==', {element, 2, '$1'}, Host},
                                {'==', '$2', ios}],
                               ['$_']}]),
    if length(Es) > 0 ->
            #jid{luser = Sender} = jlib:string_to_jid(xml:get_attr_s(<<"user">>, Attrs)),
            Message = case xml:get_subtag(Packet, <<"body">>) of
                #xmlel{} = Body ->
                    Type = <<"text">>,
                    Text = xml:get_tag_cdata(Body),
                    [{<<"type">>, Type}, {<<"message">>, Text}];
                false ->
                    case xml:get_subtag(Packet, <<"payload">>) of
                        #xmlel{} = Payload ->
                            Type = xml:get_attr_s(<<"type">>, Payload#xmlel.attrs),
                            [{<<"type">>, Type}];
                        false ->
                            abort
                    end
            end,

            case Message of
                abort ->
                    ok;
                _ ->
                    Message1 = [{<<"uid">>, Sender} | Message],
                    Tokens = [ Token || #hb_user_device{token = Token} <- Es ],
                    Message2 = jiffy:encode({Message1 ++ [{<<"devices">>, Tokens}]}),

                    Publish = #'basic.publish'{exchange = <<>>, routing_key = State#state.rabbit_queue},
                    %% ?INFO_MSG("~n~n json encoded message: ~p~n~n", [Message2]),
                    amqp_channel:cast(State#state.channel, Publish, #amqp_msg{payload = Message2})
            end;
        true ->
            ok
    end,
    {noreply, State};
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
    amqp_channel:close(State#state.channel),
    amqp_connection:close(State#state.rabbit),
    ok.

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
handle_apple_push(From, #jid{lserver = LServer} = To, Packet) ->
    Proc = gen_mod:get_module_proc(LServer, ?MODULE),
    gen_server:cast(Proc, {push_message, {From, To, Packet}}).
