-module(hblib).

-author('jerray@huoban.com').

-export([timestamp_to_integer/1,
         integer_to_timestamp/1,
         timestamp_to_xml/3,
         escape/1,
         get_bare_jid/1]).

-include("ejabberd.hrl").
-include("logger.hrl").
-include("jlib.hrl").

timestamp_to_integer({Mega, Sec, Micro}) ->
    Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.

integer_to_timestamp(IntTimestamp) when is_binary(IntTimestamp) ->
    integer_to_timestamp(jlib:binary_to_integer(IntTimestamp));
integer_to_timestamp(IntTimestamp) ->
    {IntTimestamp div 1000000000000,
     IntTimestamp div 1000000 rem 1000000,
     IntTimestamp rem 1000000}.

timestamp_to_xml(Timestamp, FromJID, Desc) ->
    T_string = jlib:now_to_utc_string(Timestamp),
    Text = [{xmlcdata, Desc}],
    From = jlib:jid_to_string(FromJID),
    #xmlel{name = <<"delay">>,
	   attrs =
	       [{<<"xmlns">>, ?NS_DELAY}, {<<"from">>, From},
		{<<"stamp">>, T_string}],
	   children = Text}.

escape(Value) when is_integer(Value) ->
    escape(jlib:integer_to_binary(Value));
escape(Value) when is_atom(Value) ->
    escape(atom_to_binary(Value, latin1));
escape(Value) ->
    ejabberd_odbc:escape(Value).

get_bare_jid(#jid{} = JID) ->
    jlib:jid_to_string(jlib:jid_remove_resource(JID));
get_bare_jid({_U, _S, _R} = JID) ->
    jlib:jid_to_string(jlib:jid_remove_resource(JID)).

