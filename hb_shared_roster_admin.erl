-module(hb_shared_roster_admin).

-behavior(gen_mod).

%% API:
-export([start/2,
         stop/1]).

%% Hooks:
-export([iq_handler/3,
         srg_create/2,
         srg_delete/2,
         srg_list/1,
         srg_get_info/2,
         srg_get_members/2,
         srg_user_add/4,
         srg_user_del/4]).

-include("ejabberd.hrl").
-include("ejabberd_commands.hrl").
-include("logger.hrl").
-include("jlib.hrl").

-define(NS_HB_SHARED_ROSTER, <<"http://huoban.com/protocol/shared_roster">>).

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
    ejabberd_commands:register_commands(commands()),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_HB_SHARED_ROSTER, ?MODULE, iq_handler, IQDisc).

stop(Host) ->
    ejabberd_commands:unregister_commands(commands()),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_HB_SHARED_ROSTER).

%%%===================================================================
%%% Hooks
%%%===================================================================
iq_handler(From, _To, #iq{type = set, sub_el = #xmlel{name = Operation, attrs = Attrs}} = IQ) ->
    #jid{lserver = LServer} = From,
    case acl:match_rule(global, configure, jlib:jid_remove_resource(From)) of
        allow ->
            Group = xml:get_attr_s(<<"org_id">>, Attrs),
            UId = xml:get_attr_s(<<"user_id">>, Attrs),
            Result = case Operation of
                <<"srg_create">> ->
                    srg_create(Group, LServer);
                <<"srg_delete">> ->
                    srg_delete(Group, LServer);
                <<"srg_user_add">> ->
                    srg_user_add(UId, LServer, Group, LServer);
                <<"srg_user_del">> ->
                    srg_user_del(UId, LServer, Group, LServer);
                _ ->
                    abort
            end,
            case Result of
                ok ->
                    IQ#iq{type=result, sub_el = []};
                abort ->
                    IQ#iq{type=error, sub_el = [?ERR_FEATURE_NOT_IMPLEMENTED]};
                _ ->
                    ?ERROR_MSG("~p fail~n~p", [Operation, IQ]),
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

srg_create(Group, Host) ->
    Opts = [{name, Group},
            {displayed_groups, [Group]}],
    case mod_shared_roster:create_group(Host, Group, Opts) of
        {atomic, ok} -> ok;
        _ -> error
    end.

srg_delete(Group, Host) ->
    case mod_shared_roster:delete_group(Host, Group) of
        {atomic, ok} -> ok;
        _ -> error
    end.

srg_list(Host) ->
    lists:sort(mod_shared_roster:list_groups(Host)).

srg_get_info(Group, Host) ->
    Opts = mod_shared_roster:get_group_opts(Host, Group),
    [{io_lib:format(<<"~p">>, [Title]),
      io_lib:format(<<"~p">>, [Value])} || {Title, Value} <- Opts].

srg_get_members(Group, Host) ->
    Members = mod_shared_roster:get_group_explicit_users(Host,Group),
    [jlib:jid_to_string(jlib:make_jid(MUser, MServer, <<"">>))
     || {MUser, MServer} <- Members].

srg_user_add(User, Host, Group, GroupHost) ->
    case mod_shared_roster:add_user_to_group(GroupHost, {User, Host}, Group) of
        {atomic, ok} -> ok;
        _ -> error
    end.

srg_user_del(User, Host, Group, GroupHost) ->
    case mod_shared_roster:remove_user_from_group(GroupHost, {User, Host}, Group) of
        {atomic, ok} -> ok;
        _ -> error
    end.

commands() ->
    [
     #ejabberd_commands{name = hb_srg_create, tags = [shared_roster_group],
                        desc = "Create a Shared Roster Group",
                        module = ?MODULE, function = srg_create,
                        args = [{group, string}, {host, string}],
                        result = {res, rescode}},
     #ejabberd_commands{name = hb_srg_delete, tags = [shared_roster_group],
                        desc = "Delete a Shared Roster Group",
                        module = ?MODULE, function = srg_delete,
                        args = [{group, string}, {host, string}],
                        result = {res, rescode}},
     #ejabberd_commands{name = hb_srg_list, tags = [shared_roster_group],
                        desc = "List the Shared Roster Groups in Host",
                        module = ?MODULE, function = srg_list,
                        args = [{host, string}],
                        result = {groups, {list, {id, string}}}},
     #ejabberd_commands{name = hb_srg_get_info, tags = [shared_roster_group],
                        desc = "Get info of a Shared Roster Group",
                        module = ?MODULE, function = srg_get_info,
                        args = [{group, string}, {host, string}],
                        result = {informations, {list, {information, {tuple, [{key, string}, {value, string}]}}}}},
     #ejabberd_commands{name = hb_srg_get_members, tags = [shared_roster_group],
                        desc = "Get members of a Shared Roster Group",
                        module = ?MODULE, function = srg_get_members,
                        args = [{group, string}, {host, string}],
                        result = {members, {list, {member, string}}}},
     #ejabberd_commands{name = hb_srg_user_add, tags = [shared_roster_group],
                        desc = "Add the JID user@host to the Shared Roster Group",
                        module = ?MODULE, function = srg_user_add,
                        args = [{user, string}, {host, string}, {group, string}, {grouphost, string}],
                        result = {res, rescode}},
     #ejabberd_commands{name = hb_srg_user_del, tags = [shared_roster_group],
                        desc = "Delete this JID user@host from the Shared Roster Group",
                        module = ?MODULE, function = srg_user_del,
                        args = [{user, string}, {host, string}, {group, string}, {grouphost, string}],
                        result = {res, rescode}}
    ].
