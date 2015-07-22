-module(hb_device).

-behavior(gen_mod).

%% API:
-export([start/2,
         stop/1]).

%% Hooks:
-export([iq_handler/3,
         bind/5,
         unbind/1,
         query_user_device/2]).

-include("ejabberd.hrl").
-include("ejabberd_commands.hrl").
-include("logger.hrl").
-include("jlib.hrl").

-define(NS_HB_DEVICE, <<"http://huoban.com/protocol/device">>).
-define(TABLE, hb_user_device).

-record(hb_user_device, {imei = <<"">>,
                         uid_host = {<<"">>, <<"">>},
                         type = ios, %% ios | android | wp
                         token = <<"">>}).

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
start(Host, Opts) ->
    IQDisc = gen_mod:get_opt(iqdisc, Opts, fun gen_iq_handler:check_type/1, one_queue),
    mnesia:create_table(?TABLE, [{disc_copies, [node()]}, 
	                             {attributes, record_info(fields, ?TABLE)}, 
	                             {type, set}]),
    mnesia:add_table_index(?TABLE, uid_host),
    mnesia:add_table_copy(?TABLE, node(), disc_copies),
    ejabberd_commands:register_commands(commands()),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_HB_DEVICE, ?MODULE, iq_handler, IQDisc).

stop(Host) ->
    ejabberd_commands:unregister_commands(commands()),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_HB_DEVICE).

%%%===================================================================
%%% Hooks
%%%===================================================================
iq_handler(From, _To, #iq{type = set, sub_el = #xmlel{name = Operation, attrs = Attrs}} = IQ) ->
    #jid{lserver = LServer} = From,
    case acl:match_rule(global, configure, jlib:jid_remove_resource(From)) of
        allow ->
            IMEI = xml:get_attr_s(<<"imei">>, Attrs),
            Result = case Operation of
                <<"bind">> ->
                    UId = xml:get_attr_s(<<"uid">>, Attrs),
                    Type = xml:get_attr_s(<<"type">>, Attrs),
                    Token = xml:get_attr_s(<<"token">>, Attrs),
                    bind(UId, LServer, IMEI, Type, Token);
                <<"unbind">> ->
                    unbind(IMEI);
                _ ->
                    abort
            end,
            case Result of
                ok ->
                    ?INFO_MSG("~p ok IMEI: ~p", [Operation, IMEI]),
                    IQ#iq{type=result, sub_el = []};
                abort ->
                    IQ#iq{type=error, sub_el = [?ERR_FEATURE_NOT_IMPLEMENTED]};
                Err ->
                    ?ERROR_MSG("~p fail IMEI: ~p, reason: ~p", [Operation, IMEI, Err]),
                    IQ#iq{type=error, sub_el = [?ERR_INTERNAL_SERVER_ERROR]}
            end;
        deny ->
            IQ#iq{type=error,sub_el = [?ERR_FORBIDDEN]}
    end;

iq_handler(_From, _To, IQ)->
    IQ#iq{type=error, sub_el = [?ERR_NOT_ALLOWED]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
bind(UId, Host, IMEI, Type, Token) ->
    F = fun() ->
            mnesia:write(#hb_user_device{imei = format_arg(IMEI),
                                         uid_host = {format_arg(UId), format_arg(Host)},
                                         type = convert_type(format_arg(Type)),
                                         token = format_arg(Token)})
    end,
    case mnesia:transaction(F) of
        {atomic, ok} -> ok;
        _ -> error
    end.

unbind(IMEI) ->
    F = fun() ->
            mnesia:delete({?TABLE, format_arg(IMEI)}),
            ok
    end,
    case mnesia:transaction(F) of
        {atomic, ok} -> ok;
        _ -> error
    end.

query_user_device(User, Host) ->
    Es = mnesia:dirty_select(hb_user_device,
                             [{#hb_user_device{imei = '_', uid_host = '$1', type = '$2', _ = '_'},
                               [{'==', {element, 1, '$1'}, format_arg(User)},
                                {'==', {element, 2, '$1'}, format_arg(Host)}],
                               ['$_']}]),
    lists:map(fun (#hb_user_device{imei = IMEI, type = Type, token = Token}) ->
                io_lib:format("IMEI:~s Type:~p Token:~s~n", [binary_to_list(IMEI), Type, binary_to_list(Token)])
        end, Es).

format_arg(Val) when is_list(Val) -> list_to_binary(Val);
format_arg(Val) when is_binary(Val) -> Val;
format_arg(Val) when is_atom(Val) -> Val.

convert_type(<<"ios">>) -> ios;
convert_type(<<"android">>) -> android;
convert_type(<<"wp">>) -> wp;
convert_type(_) -> unknown.

commands() ->
    [
     #ejabberd_commands{name = hb_bind_device, tags = [huoban],
                        desc = "Bind a mobile device",
                        longdesc = "",
                        module = ?MODULE, function = bind,
                        args = [{uid, string}, {host, string}, {imei, string}, {type, string}, {token, string}],
                        result = {result, atom}},
     #ejabberd_commands{name = hb_unbind_device, tags = [huoban],
                        desc = "Unbind a mobile device",
                        longdesc = "",
                        module = ?MODULE, function = unbind,
                        args = [{imei, string}],
                        result = {result, atom}},
     #ejabberd_commands{name = hb_user_devices, tags = [huoban],
                        desc = "Query a user's mobile devices",
                        longdesc = "",
                        module = ?MODULE, function = query_user_device,
                        args = [{uid, string}, {host, string}],
                        result = {list, string}}
    ].
