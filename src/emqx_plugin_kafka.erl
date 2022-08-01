%%--------------------------------------------------------------------
%% Copyright (c) 2015-2017 Feng Lee <feng@emqtt.io>.
%%
%% Modified by Ramez Hanna <rhanna@iotblue.net>
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

% -include("emqx_plugin_kafka.hrl").

% -include_lib("emqx/include/emqx.hrl").

-include("emqx.hrl").
-include_lib("kernel/include/logger.hrl").

-export([load/1, unload/0]).

%% Client Lifecircle Hooks
-export([on_client_connect/3
  , on_client_connack/4
  , on_client_connected/3
  , on_client_disconnected/4
  , on_client_authenticate/3
  , on_client_check_acl/5
  , on_client_subscribe/4
  , on_client_unsubscribe/4
]).

%% Session Lifecircle Hooks
-export([on_session_created/3
  , on_session_subscribed/4
  , on_session_unsubscribed/4
  , on_session_resumed/3
  , on_session_discarded/3
  , on_session_takeovered/3
  , on_session_terminated/4
]).

%% Message Pubsub Hooks
-export([on_message_publish/2
  , on_message_delivered/3
  , on_message_acked/3
  , on_message_dropped/4
]).


%% Called when the plugin application start
load(Env) ->
  ekaf_init([Env]),
  emqx:hook('client.connect', {?MODULE, on_client_connect, [Env]}),
  emqx:hook('client.connack', {?MODULE, on_client_connack, [Env]}),
  emqx:hook('client.connected', {?MODULE, on_client_connected, [Env]}),
  emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
  emqx:hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
  emqx:hook('client.check_acl', {?MODULE, on_client_check_acl, [Env]}),
  emqx:hook('client.subscribe', {?MODULE, on_client_subscribe, [Env]}),
  emqx:hook('client.unsubscribe', {?MODULE, on_client_unsubscribe, [Env]}),
  emqx:hook('session.created', {?MODULE, on_session_created, [Env]}),
  emqx:hook('session.subscribed', {?MODULE, on_session_subscribed, [Env]}),
  emqx:hook('session.unsubscribed', {?MODULE, on_session_unsubscribed, [Env]}),
  emqx:hook('session.resumed', {?MODULE, on_session_resumed, [Env]}),
  emqx:hook('session.discarded', {?MODULE, on_session_discarded, [Env]}),
  emqx:hook('session.takeovered', {?MODULE, on_session_takeovered, [Env]}),
  emqx:hook('session.terminated', {?MODULE, on_session_terminated, [Env]}),
  emqx:hook('message.publish', {?MODULE, on_message_publish, [Env]}),
  emqx:hook('message.delivered', {?MODULE, on_message_delivered, [Env]}),
  emqx:hook('message.acked', {?MODULE, on_message_acked, [Env]}),
  emqx:hook('message.dropped', {?MODULE, on_message_dropped, [Env]}).

on_client_connect(ConnInfo = #{clientid := ClientId}, Props, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connect, ConnInfo: ~p, Props: ~p~n",
    [ClientId, ConnInfo, Props]),
  ok.

on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n",
    [ClientId, ConnInfo, Rc, Props]),
  ok.

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ClientInfo, ConnInfo]),
  {IpAddr, _Port} = maps:get(peername, ConnInfo),
  Action = <<"connected">>,
  Now = now_mill_secs(os:timestamp()),
  Online = 1,
  Payload = [
    {action, Action},
    {clientid, ClientId},
    {username, maps:get(username, ClientInfo)},
    {keepalive, maps:get(keepalive, ConnInfo)},
    {ipaddress, iolist_to_binary(ntoa(IpAddr))},
    {proto_name, maps:get(proto_name, ConnInfo)},
    {proto_ver, maps:get(proto_ver, ConnInfo)},
    {timestamp, Now},
    {online, Online}
  ],
  Topic = list_to_binary([brod_get_topic(), <<"status">>]),
  {ok, MessageBody} = emqx_json:safe_encode(Payload),
  produce_kafka_payload(Topic, Action, MessageBody),
  ok.

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ReasonCode, ClientInfo, ConnInfo]),
  Action = <<"disconnected">>,
  Now = now_mill_secs(os:timestamp()),
  Online = 0,
  Payload = [
    {action, Action},
    {clientid, ClientId},
    {username, maps:get(username, ClientInfo)},
    {reason, ReasonCode},
    {timestamp, Now},
    {online, Online}
  ],
  Topic = list_to_binary([brod_get_topic(), <<"status">>]),
  {ok, MessageBody} = emqx_json:safe_encode(Payload),
  produce_kafka_payload(Topic, Action, MessageBody),
  ok.

on_client_authenticate(_ClientInfo = #{clientid := ClientId}, Result, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) authenticate, Result:~n~p~n", [ClientId, Result]),
  ok.

on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n",
    [ClientId, PubSub, Topic, Result]),
  ok.

%%---------------------------client subscribe start--------------------------%%
on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
  ok.
%%---------------------client subscribe stop----------------------%%
on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
  ok.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
  ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Message dropped by node ~s due to ~s: ~s~n",
    [Node, Reason, emqx_message:format(Message)]),
  ok.


%%---------------------------message publish start--------------------------%%
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  ok;
on_message_publish(Message, _Env) ->
  %%?LOG_INFO("[KAFKA PLUGIN]Client publish before produce, Message:~n~p~n",[Message]),
  %%{ok, Payload} = format_payload(Message),
  %%get_kafka_topic_produce(Message#message.topic, Payload),
  %% produce_kafka_payload(<<"publish">>, Message#message.topic, Payload),
  %%?LOG_INFO("[KAFKA PLUGIN]Client publish after produce, Payload:~n~p~n, Topic:~n~p~n",[Payload, Message#message.topic]),
  ok.
%%---------------------message publish stop----------------------%%

on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Message delivered to client(~s): ~s~n",
    [ClientId, emqx_message:format(Message)]),
  Topic = Message#message.topic,
  Payload = Message#message.payload,
  get_kafka_topic_produce(Topic, Payload),
  ok.

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Message acked by client(~s): ~s~n",
    [ClientId, emqx_message:format(Message)]),
  Topic = Message#message.topic,
  Payload = Message#message.payload,
  get_kafka_topic_produce(Topic, Payload),
  ok.

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]),
  ok.


on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]),
  ok.

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]),
  ok.

on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]),
  ok.

on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]),
  ok.

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]),
  ok.

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) is terminated due to ~p~nSession Info: ~p~n",
    [ClientId, Reason, SessInfo]),
  ok.

ekaf_init(_Env) ->
  io:format("Init emqx plugin kafka....."),
  {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
  KafkaHost = proplists:get_value(host, BrokerValues),
  ?LOG_INFO("[KAFKA PLUGIN]KafkaHost = ~s~n", [KafkaHost]),
  KafkaPort = proplists:get_value(port, BrokerValues),
  ?LOG_INFO("[KAFKA PLUGIN]KafkaPort = ~s~n", [KafkaPort]),
  KafkaPartitionStrategy = proplists:get_value(partitionstrategy, BrokerValues),
  KafkaPartitionWorkers = proplists:get_value(partitionworkers, BrokerValues),
  KafkaTopic = proplists:get_value(payloadtopic, BrokerValues),
  ?LOG_INFO("[KAFKA PLUGIN]KafkaTopic = ~s~n", [KafkaTopic]),
  application:set_env(ekaf, ekaf_bootstrap_broker, {KafkaHost, list_to_integer(KafkaPort)}),
  application:set_env(ekaf, ekaf_partition_strategy, list_to_atom(KafkaPartitionStrategy)),
  application:set_env(ekaf, ekaf_per_partition_workers, KafkaPartitionWorkers),
  application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(KafkaTopic)),
  application:set_env(ekaf, ekaf_buffer_ttl, 10),
  application:set_env(ekaf, ekaf_max_downtime_buffer_size, 5),
  % {ok, _} = application:ensure_all_started(kafkamocker),
  {ok, _} = application:ensure_all_started(gproc),
  % {ok, _} = application:ensure_all_started(ranch),
  {ok, _} = application:ensure_all_started(ekaf).

%% Called when the plugin application stop
unload() ->
  emqx:unhook('client.connect', {?MODULE, on_client_connect}),
  emqx:unhook('client.connack', {?MODULE, on_client_connack}),
  emqx:unhook('client.connected', {?MODULE, on_client_connected}),
  emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
  emqx:unhook('client.authenticate', {?MODULE, on_client_authenticate}),
  emqx:unhook('client.check_acl', {?MODULE, on_client_check_acl}),
  emqx:unhook('client.subscribe', {?MODULE, on_client_subscribe}),
  emqx:unhook('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
  emqx:unhook('session.created', {?MODULE, on_session_created}),
  emqx:unhook('session.subscribed', {?MODULE, on_session_subscribed}),
  emqx:unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
  emqx:unhook('session.resumed', {?MODULE, on_session_resumed}),
  emqx:unhook('session.discarded', {?MODULE, on_session_discarded}),
  emqx:unhook('session.takeovered', {?MODULE, on_session_takeovered}),
  emqx:unhook('session.terminated', {?MODULE, on_session_terminated}),
  emqx:unhook('message.publish', {?MODULE, on_message_publish}),
  emqx:unhook('message.delivered', {?MODULE, on_message_delivered}),
  emqx:unhook('message.acked', {?MODULE, on_message_acked}),
  emqx:unhook('message.dropped', {?MODULE, on_message_dropped}).



ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
  inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
  inet_parse:ntoa(IP).
now_mill_secs({MegaSecs, Secs, _MicroSecs}) ->
  MegaSecs * 1000000000 + Secs * 1000 + _MicroSecs.
brod_get_topic() ->
  {ok, Kafka} = application:get_env(?MODULE, broker),
  Topic_prefix = proplists:get_value(payloadtopic, Kafka),
  Topic_prefix.

getPartition(Key) ->
  {ok, Kafka} = application:get_env(?MODULE, broker),
  PartitionNum = proplists:get_value(partition, Kafka),
  <<Fix:120, Match:8>> = crypto:hash(md5, Key),
  abs(Match) rem PartitionNum.

produce_kafka_message(Topic, Message, ClientId, Env) ->
  Key = iolist_to_binary(ClientId),
  Partition = getPartition(Key),
  %%JsonStr = jsx:encode(Message),
  Kafka = proplists:get_value(broker, Env),
  IsAsyncProducer = proplists:get_value(is_async_producer, Kafka),
  if
    IsAsyncProducer == false ->
      brod:produce_sync(brod_client_1, Topic, Partition, ClientId, Message);
    true ->
      brod:produce_no_ack(brod_client_1, Topic, Partition, ClientId, Message)
%%      brod:produce(brod_client_1, Topic, Partition, ClientId, JsonStr)
  end,
  ok.

get_kafka_topic_produce(Topic, Message, Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Kafka topic = -s-n", [Topic]),
  TopicPrefix = string:left(binary_to_list(Topic),6),
  TlinkFlag = string:equal(TopicPrefix, <<"tlink/">>),
  if
    TlinkFlag == true ->
      TopicStr = binary_to_list(Topic),
      OtaIndex = string:str(TopicStr,"ota"),
      SubRegisterIndex = string:str(TopicStr,"sub/register"),
      SubLogin = string:str(TopicStr,"sub/login"),
      if
        OtaIndex /= 0 ->
          TopicKafka = list_to_binary([brod_get_topic(), <<"ota">>]);
        SubRegisterIndex /= 0 ->
          TopicKafka = list_to_binary([brod_get_topic(), <<"sub_register">>]);
        SubLogin /= 0 ->
          TopicKafka = list_to_binary([brod_get_topic(), <<"sub_status">>]);
        true ->
          TopicKafka = list_to_binary([brod_get_topic(), <<"msg">>])
      end,
      produce_kafka_message(TopicKafka, Message, Topic, Env);
    TlinkFlag == false ->
      ?LOG_INFO("[KAFKA PLUGIN]MQTT topic prefix is not tlink = ~s~n",[Topic])
  end,
  ok.

kafka_consume(Topics)->
  GroupConfig = [{offset_commit_policy, commit_to_kafka_v2},
    {offset_commit_interval_seconds, 5}
  ],
  GroupId = <<"emqx_cluster">>,
  ConsumerConfig = [{begin_offset, earliest}],
  brod:start_link_group_subscriber(brod_client_1, GroupId, Topics,
    GroupConfig, ConsumerConfig,
    _CallbackModule  = ?MODULE,
    _CallbackInitArg = []).

handle_message(_Topic, Partition, Message, State) ->
  #kafka_message{ offset = Offset
    , key   = Key
    , value = Value
  } = Message,
  error_logger:info_msg("~p ~p: offset:~w key:~s value:~s\n",
    [self(), Partition, Offset, Key, Value]),
  TopicStr = binary_to_list(_Topic),
  MsgDown = string:equal(TopicStr,"tlink_device_msg_down"),
  KickOut = string:equal(TopicStr,"tlink_device_kick_out"),
  AclClean = string:equal(TopicStr,"tlink_device_acl_clean"),
  if
    MsgDown ->
      handle_message_msg_down(Key, Value);
    KickOut ->
      {ok, UserName} = get_username(Value),
      {ok, ClientList} = get_client_list_by_username(UserName),
      kick_out(ClientList);
    AclClean ->
      {ok, UserName} = get_username(Value),
      {ok, ClientList} = get_client_list_by_username(UserName),
      acl_clean(ClientList);
    true ->
      error_logger:error_msg("the kafka comsume topic is not exist\n")
  end,
  error_logger:info_msg("message:~p\n",[Message]),
  {ok, ack, State}.

handle_message_msg_down(Key, Value) ->
  Topic = Key,
  Payload = Value,
  Qos = 0,
  Msg = emqx_message:make(<<"consume">>, Qos, Topic, Payload),
  emqx_broker:safe_publish(Msg),
  ok.

get_username(Value) ->
  {ok,Term} =  emqx_json:safe_decode(Value),
  Map = maps:from_list(Term),
  %% 获取消息体中数据
  MessageUserName = maps:get(<<"username">>, Map),
  MessageProductKey = maps:get(<<"username">>, Map),
  MessageDeviceName = maps:get(<<"username">>, Map),
  %% 获取数据是否存在
  MessageUserNameLen = string:len(binary_to_list(MessageUserName)),
  MessageProductKeyLen = string:len(binary_to_list(MessageProductKey)),
  MessageDeviceNameLen = string:len(binary_to_list(MessageDeviceName)),
  %% 确定最终username
  if
    MessageUserNameLen /= 0 ->
      UserName = MessageUserName;
    MessageProductKeyLen /= 0, MessageDeviceNameLen/= 0 ->
      UserName = list_to_binary([MessageDeviceName, <<"&">>, MessageProductKey]);
    true ->
      UserName = <<"">>
  end,
  {ok, UserName}.

get_client_list_by_username(UserName) ->
  UserNameLen = string:len(binary_to_list(UserName)),
  if
    UserNameLen /= 0 ->
      UserNameMap = #{
        username => UserName
      },
      {ok, Client} = emqx_mgmt_api_clients:lookup(UserNameMap, {}),
      %% 获取客户端list
      ClientList = maps:get(data, Client);
    true ->
      ClientList = [],
      error_logger:error_msg("the kafka comsume kick out username is not exist")
  end,
  {ok, ClientList}.

kick_out(ClientList) ->
  ClientListLength = length(ClientList),
  if
    ClientListLength > 0 ->
      %% 遍历剔除clientid
      lists:foreach(fun(C) ->
        ClientId = maps:get(clientid, C),
        ClientIdMap = #{
          clientid => ClientId
        },
        emqx_mgmt_api_clients:kickout(ClientIdMap, {}) end,
        ClientList
      );
    true ->
      error_logger:error_msg("the kafka comsume kick out clientlist length is zero")
  end,
  ok.

acl_clean(ClientList) ->
  ClientListLength = length(ClientList),
  if
    ClientListLength > 0 ->
      %% 遍历剔除clientid
      lists:foreach(fun(C) ->
        ClientId = maps:get(clientid, C),
        ClientIdMap = #{
          clientid => ClientId
        },
        emqx_mgmt_api_clients:clean_acl_cache(ClientIdMap, {}) end,
        ClientList
      );
    true ->
      error_logger:error_msg("the kafka comsume acl clean clientlist length is zero")
  end,
  ok.

