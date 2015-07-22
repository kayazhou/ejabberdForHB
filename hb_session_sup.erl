-module(hb_session_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_child/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Args) when is_list(Args) ->
    supervisor:start_child(?MODULE, Args).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    Session = {undefined, {hb_session, start_link, []},
               transient, 1000, worker, [hb_session]},
    RestartStrategy = {simple_one_for_one, 10, 1},
    {ok, {RestartStrategy, [Session]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
