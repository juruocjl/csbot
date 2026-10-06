"""Image identity evidence: names, ambiguity, boundaries and the actual dispatch."""
import ast
import copy
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.image_identity import SQL, with_image_identities
from ai_runtime.queries import compile_query

class Checks(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.image={'storage':'snapshot','metadata':{'snapshot':{'players':[{'steamId':'s1','nickname':'同名'}]},'image_id':'immutable'}}
        self.rows=[{'steamid':'s1','uid':'11','steam_nickname':'同名','cached_qq_nickname':'旧甲','steam_updated_at':1,'qq_updated_at':1},
                   {'steamid':'s2','uid':'22','steam_nickname':'同名','cached_qq_nickname':'旧乙','steam_updated_at':1,'qq_updated_at':1}]
        self.broker=SimpleNamespace(query=AsyncMock(return_value={'rows':self.rows,'truncated':False,'fetched_at':'now','source':'main'}))
        self.group=SimpleNamespace(members=AsyncMock(return_value=[{'uid':'11','nickname':'甲QQ','card':'甲群名片'},{'uid':'22','nickname':'乙QQ','card':'乙群名片'}]),_members_checked='now')
    async def read(self):return await with_image_identities(self.image,self.broker,self.group)
    async def test_same_game_nickname_is_resolved_by_steam_id_and_live_card(self):
        before=copy.deepcopy(self.image);value=await self.read()
        selected=[r for r in value['identity_context']['bindings'] if r['steamid']==value['metadata']['snapshot']['players'][0]['steamId']]
        self.assertEqual([(r['uid'],r['qq_card']) for r in selected],[('11','甲群名片')])
        self.assertEqual(self.image,before);self.assertEqual(value['identity_context']['basis'],'current_group_bindings')
        self.group.members.return_value[0]['card']='新群名片'
        self.assertEqual((await self.read())['identity_context']['bindings'][0]['qq_card'],'新群名片')
    async def test_multiple_bindings_and_incomplete_results_are_not_collapsed(self):
        self.rows.append(self.rows[1] | {'steamid':'s1'})
        self.broker.query.return_value['truncated']=True
        value=(await self.read())['identity_context']
        self.assertEqual(len([r for r in value['bindings'] if r['steamid']=='s1']),2)
        self.assertTrue(value['truncated'])
        self.group.members.return_value=[]
        missing=(await self.read())['identity_context']['bindings'][0]
        self.assertFalse(missing['in_current_qq_group']);self.assertIsNone(missing['qq_nickname'])
    async def test_failed_lookups_are_explicit_and_do_not_discard_metadata(self):
        self.group.members.side_effect=ValueError('QQ offline')
        value=(await self.read())['identity_context']
        self.assertEqual(value['qq_names_source'],'database_cache')
        self.assertIsNone(value['bindings'][0]['qq_card']);self.assertIsNone(value['bindings'][0]['in_current_qq_group'])
        self.broker.query.side_effect=ValueError('private connection detail')
        result=await self.read();self.assertEqual(result['identity_context']['status'],'unavailable')
        self.assertNotIn('private',str(result));self.assertEqual(result['metadata'],self.image['metadata'])
    async def test_ordinary_image_skips_lookup_and_sql_keeps_all_group_filters(self):
        raw={'full_status':'available'}
        self.assertIs(await with_image_identities(raw,self.broker,self.group),raw)
        self.broker.query.assert_not_called()
        statement=compile_query(SQL,{},'123')
        self.assertIn("g.gid = '123'",statement)
        self.assertIn("group_members",statement)
        self.assertIn("members_steamid",statement)
        self.assertIn("WHERE gid = '123'",statement)
    async def test_teammate_metadata_keeps_subject_id_and_all_teammate_ids(self):
        import tempfile
        from unicodedata import normalize
        path=Path(__file__).resolve().parents[1]/'plugins/cs_img/__init__.py'
        node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='gen_teammate_image')
        capture=AsyncMock(return_value='pixels')
        env=dict(tempfile=tempfile,normalize=normalize,avatar_dir=Path('/synthetic'),path_to_file_url=str,
                 teammate_content=['start','_title_ _avatar_ _name_ _content_','end'],screenshot_html_to_png=capture)
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        await env['gen_teammate_image']('subject','本赛季',[('首项','s1','同名','1'),('末项','s2','同名','2')],archive_data=[])
        self.assertEqual(capture.call_args.kwargs['archive_source']['steamid'],'subject')
        self.assertEqual([row[1] for row in capture.call_args.kwargs['archive_snapshot']['rows']],['s1','s2'])
    async def test_scoretrend_records_explicit_qq_and_steam_ids(self):
        import warnings
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime
        from io import BytesIO
        path=Path(__file__).resolve().parents[1]/'plugins/cs/__init__.py'
        node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='scoretrend_function')
        node.decorator_list=[]
        capture=lambda pixels,title,snapshot: snapshot
        database=SimpleNamespace(get_steamid=AsyncMock(return_value='s1'),get_pvp_score_history=AsyncMock(return_value=[(1700000000,2500)]),get_base_info=AsyncMock(return_value=SimpleNamespace(name='同名')))
        matcher=SimpleNamespace(send=AsyncMock(),finish=AsyncMock())
        env=dict(MessageEvent=object,Message=object,CommandArg=lambda:None,datetime=datetime,
                 parse_target_args=lambda _: (['11'],[]),valid_time=['本赛季'],db_val=database,
                 plt=plt,mdates=mdates,BytesIO=BytesIO,scoretrend=matcher,image_segment=lambda value:value)
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        from unittest.mock import patch
        with patch('ai_runtime.image_archive.snapshot_image',side_effect=capture),warnings.catch_warnings():
            warnings.filterwarnings('ignore',message='Glyph .* missing from font')
            await env['scoretrend_function'](SimpleNamespace(get_user_id=lambda:'11'))
        data=matcher.finish.call_args.args[0]['series'][0]
        self.assertEqual((data['qq'],data['steamid'],data['nickname']),('11','s1','同名'))
        self.assertEqual(data['points'][0]['score'],2500)
    async def test_actual_image_tool_dispatch_attaches_identity_evidence(self):
        path=Path(__file__).resolve().parents[1]/'ai_runtime/service.py'
        node=next(n for n in ast.walk(ast.parse(path.read_text())) if isinstance(n,ast.AsyncFunctionDef) and n.name=='dispatch')
        get_image=AsyncMock(return_value=self.image)
        env=dict(authorized_image=get_image,factory=None,gid='123',broker=self.broker,group=self.group,
                 with_image_identities=with_image_identities,pin_image=lambda value:value)
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        result=await env['dispatch']({'method':'image','id':'fixture'})
        self.assertEqual(result['identity_context']['bindings'][0]['uid'],'11')
        get_image.assert_awaited_once_with(None,'123','fixture')

if __name__=='__main__':unittest.main()
