"""Synthetic scheduler/delivery integration, without network, database or QQ."""
import ast
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from nonebot.adapters.onebot.v11 import Message, MessageSegment
from nonebot.exception import ActionFailed

PLUGIN = Path(__file__).parents[1] / 'plugins' / 'hltv_watcher'
spec = importlib.util.spec_from_file_location('watcher_runtime_logic', PLUGIN / 'logic.py')
LOGIC = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = LOGIC
spec.loader.exec_module(LOGIC)
package = SimpleNamespace(__path__=[str(PLUGIN)])
sys.modules['watcher_runtime'] = package
sys.modules['watcher_runtime.logic'] = LOGIC
spec = importlib.util.spec_from_file_location('watcher_runtime.schedule_logic', PLUGIN / 'schedule_logic.py')
PARSER = importlib.util.module_from_spec(spec); spec.loader.exec_module(PARSER)

FUNCTIONS = {'_load_int','_save_int','_load_metadata','_get_bot_or_none','_followed_teams',
             'refresh_watch_list','_poll_event','_poll_due_events','_send_pending_notifications',
             '_discover_followed_team','_rebuild_watch_list'}

class ApiError(RuntimeError):
    def __init__(self, message, status=None): super().__init__(message); self.status=status

class Storage:
    def __init__(self): self.data={}
    async def set(self,key,value): self.data[key]=value
    async def get(self,key,default=None): return self.data.get(key,default)

class Config:
    def __init__(self): self.values={'hltv_event_id_list':[], 'hltv_followed_teams':['TYLOO'], 'hltv_manual_event_ids':[]}
    async def get(self,key): return self.values[key]
    async def set(self,key,value,_): self.values[key]=value


def event(eid, status='live'):
    now=int(time.time())
    return LOGIC.EventInfo(event_id=eid,name=f'赛事{eid}',status=status,grade='1',
        start_ts=now + (3600 if status=='upcoming' else -3600),end_ts=now+86400)


def load():
    nodes=[]
    for node in ast.parse((PLUGIN/'__init__.py').read_text()).body:
        if isinstance(node,ast.Assign) and all(isinstance(t,ast.Name) and t.id.isupper() for t in node.targets): nodes.append(node)
        elif isinstance(node,ast.AsyncFunctionDef) and node.name in FUNCTIONS:
            node.decorator_list=[]; nodes.append(node)
    storage, config = Storage(), Config()
    store=SimpleNamespace(save_events=AsyncMock(),watched=AsyncMock(return_value=([],set())),has_catalog=AsyncMock(return_value=True),
        default_ids=AsyncMock(return_value={'team-a'}),ingest=AsyncMock(),
        due_team=AsyncMock(return_value=None),delay_team=AsyncMock(),
        claim_delivery=AsyncMock(return_value=None),delivery_result=AsyncMock())
    ns={'asyncio':SimpleNamespace(sleep=AsyncMock()),'json':json,'time':time,'random':__import__('random'),
        'logger':Mock(),'local_storage':storage,'runtime_config':config,'schedule_store':store,
        'config':SimpleNamespace(cs_group_list=[100],cs_event_group_list=[100,200]),
        'Bot':object,'Message':Message,'MessageSegment':MessageSegment,'ActionFailed':ActionFailed,
        'DEFAULT_FOLLOWED_TEAMS':LOGIC.DEFAULT_FOLLOWED_TEAMS,'EventInfo':LOGIC.EventInfo,
        'events_from_json':LOGIC.events_from_json,'events_to_json':LOGIC.events_to_json,
        'event_is_active':LOGIC.event_is_active,
        'should_poll_event':LOGIC.should_poll_event,'poll_schedule':{},'FiveEApiError':ApiError,
        'legacy_results':PARSER.legacy_results,'event_update':AsyncMock(),'fetch_events':AsyncMock(),
        'get_event_schedule':AsyncMock(return_value=(None, [])),'get_bot':Mock()}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(PLUGIN/'__init__.py'),'exec'),ns)
    return ns,storage,config,store


class RuntimeAcceptance(unittest.IsolatedAsyncioTestCase):
    async def test_failed_global_discovery_retains_old_events_during_partial_team_refresh(self):
        ns,storage,config,store=load()
        storage.data['hltv_discovery_failures']='1'
        storage.data['hltv_event_meta']=LOGIC.events_to_json([event(2)])
        config.values['hltv_event_id_list']=[2]
        store.watched.return_value=([event(1)],set())
        await ns['_rebuild_watch_list']()
        self.assertEqual(sorted(config.values['hltv_event_id_list']),[1,2])
        storage.data['hltv_discovery_failures']='0'
        await ns['_rebuild_watch_list']()
        self.assertEqual(config.values['hltv_event_id_list'],[1])

    async def test_discovery_persists_derived_watch_list_and_preserves_failure_cache(self):
        ns,storage,config,store=load()
        selected=[event(1),event(2,'upcoming')]
        ns['fetch_events'].return_value=selected
        store.watched.return_value=(selected,{'team-a'})
        await ns['refresh_watch_list'](True)
        self.assertEqual(config.values['hltv_event_id_list'],[1,2])
        store.save_events.assert_awaited_once_with(selected)
        ns['fetch_events'].side_effect=ApiError('blocked',429)
        await ns['refresh_watch_list'](True)
        self.assertEqual(config.values['hltv_event_id_list'],[1,2])
        self.assertGreater(int(storage.data['hltv_poll_cooldown']),int(time.time()))

    async def test_new_follow_uses_complete_provider_id_to_discover(self):
        ns,storage,config,store=load()
        store.due_team.return_value=('fixed-internal-id','hltv_team_123')
        ns['fetch_events'].return_value=[event(7)]
        await ns['_discover_followed_team']()
        ns['fetch_events'].assert_awaited_once_with(team_id='hltv_team_123')
        store.save_events.assert_awaited_once_with([event(7)])
        store.delay_team.assert_awaited_once_with('fixed-internal-id',1800)

    async def test_all_states_ingested_and_legacy_results_remain_idempotent(self):
        ns,storage,config,store=load()
        raw={'mc_info':{'id':'m1','plan_ts':'1','t1_info':{'id':'csgo_tm_1','disp_name':'A'},'t2_info':{'id':'hltv_team_1','disp_name':'B'}},
             'tt_info':{'id':'csgo_tt_1'},'state':{'status':'2','t1_score':'0','t2_score':'2'}}
        ns['get_event_schedule'].return_value=(None, PARSER.parse_schedule_matches(1,[raw]))
        await ns['_poll_event'](event(1),['TYLOO'],Mock())
        await ns['_poll_event'](event(1),['TYLOO'],Mock())
        self.assertEqual(json.loads(storage.data['hltvresult1']),[['B','A','2:0','m1']])
        self.assertEqual(ns['event_update'].await_count,1)
        self.assertEqual(store.ingest.await_count,2)
        self.assertEqual(store.ingest.await_args.args[-1],{'m1'})
        raw['state']['t1_score']='1'
        ns['get_event_schedule'].return_value=(None, PARSER.parse_schedule_matches(1,[raw]))
        await ns['_poll_event'](event(1),[],Mock())
        self.assertEqual(ns['event_update'].await_count,2)

    async def test_upcoming_polled_then_uses_thirty_minute_interval(self):
        ns,storage,config,store=load(); now=int(time.time())
        events=[event(20),event(21,'upcoming')]
        config.values['hltv_event_id_list']=[20,21]
        storage.data['hltv_event_meta']=LOGIC.events_to_json(events)
        await ns['_poll_due_events'](Mock(),now)
        await ns['_poll_due_events'](Mock(),now+31)
        self.assertEqual(ns['get_event_schedule'].await_args.args[0],20)
        await ns['_poll_due_events'](Mock(),now+170)
        self.assertEqual(ns['get_event_schedule'].await_args.args[0],21)
        self.assertGreater(ns['poll_schedule'][21],now+1700)

    async def test_new_result_preserves_older_major_feed_order(self):
        ns,storage,config,store=load()
        storage.data['hltvresult1']=json.dumps([['B','A','2:0','m2'],['B','A','2:0','m1']])
        raw={'mc_info':{'id':'m3','plan_ts':'3','t1_info':{'disp_name':'A'},'t2_info':{'disp_name':'B'}},
             'tt_info':{'id':'csgo_tt_1'},'state':{'status':'2','t1_score':'0','t2_score':'2'}}
        ns['get_event_schedule'].return_value=(None,PARSER.parse_schedule_matches(1,[raw]))
        await ns['_poll_event'](event(1),[],Mock())
        self.assertEqual([m[3] for m in json.loads(storage.data['hltvresult1'])],['m3','m2','m1'])

    async def test_live_polls_stagger_and_rate_limit_stops_cycle(self):
        ns,storage,config,store=load(); now=int(time.time())
        events=[event(100+i) for i in range(4)]
        config.values['hltv_event_id_list']=[e.event_id for e in events]
        storage.data['hltv_event_meta']=LOGIC.events_to_json(events)
        await ns['_poll_due_events'](Mock(),now)
        for i,offset in enumerate([31,98,170,240]):
            await ns['_poll_due_events'](Mock(),now+offset)
            self.assertEqual(ns['get_event_schedule'].await_count,i+1)
        storage.data['hltv_poll_cooldown']=str(now+2000)
        await ns['_poll_due_events'](Mock(),now+600)
        self.assertEqual(ns['get_event_schedule'].await_count,4)

    async def test_delivery_uses_one_group_message_and_real_at_segments(self):
        ns,storage,config,store=load()
        delivery={'key':('m1','100',1),'text':'A 2:1 B','users':['111','222'],'group_id':'100'}
        store.claim_delivery.side_effect=[delivery,None]
        bot=SimpleNamespace(send_group_msg=AsyncMock(return_value={'message_id':1}))
        await ns['_send_pending_notifications'](bot)
        bot.send_group_msg.assert_awaited_once()
        segments=bot.send_group_msg.await_args.kwargs['message']
        self.assertEqual([s.data['qq'] for s in segments if s.type=='at'],['111','222'])
        store.delivery_result.assert_awaited_once_with(delivery['key'],'sent')

    async def test_network_timeout_is_unknown_without_automatic_resend(self):
        ns,storage,config,store=load()
        delivery={'key':('m1','100',1),'text':'A 2:1 B','users':[],'group_id':'100'}
        store.claim_delivery.side_effect=[delivery,None]
        bot=SimpleNamespace(send_group_msg=AsyncMock(side_effect=TimeoutError()))
        await ns['_send_pending_notifications'](bot)
        store.delivery_result.assert_awaited_once_with(delivery['key'],'unknown')

if __name__=='__main__': unittest.main()
