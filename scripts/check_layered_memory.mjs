import {test} from 'node:test';
import assert from 'node:assert/strict';
import {configureMemory,wrapMemoryTool,foundationalContext,saveSummary,sources,unpack} from '../ai_runtime/dsh/layered-memory.mjs';
const db=new Map();let count=0,fail=false;
const active=()=>[...db.values()].filter(x=>!x.forgotten);
const register=(name,execute)=>wrapMemoryTool({name,parameters:{},output:{},execute});
register('memory_list',async ({offset=0,limit=200})=>({items:active().slice(offset,offset+limit),total:active().length}));
const search=register('memory_search',async ({query,limit=20})=>({items:active().filter(x=>JSON.stringify(x).includes(query)).slice(0,limit)}));
register('memory_get',async ({id})=>({memory:db.get(id)}));
const forget=register('memory_forget',async ({id})=>{db.get(id).forgotten=true;return {memory:{id,forgotten:true}};});
const save=register('memory_save',async args=>{if(fail)throw Error('disk unavailable');const id=String(++count);db.set(id,{...args,id,updated_at:String(count).padStart(6,'0')});return {action:'created',id};});
const exec={agent:{session:{id:'fixture'}}};
let approved=true;
function setup(text='小茶是QQ 22222（茶茶）'){configureMemory({inputSources:()=>[{id:'source',kind:'user',text}],events:()=>[],verify:async()=>approved});}
const fact={title:'小茶',content:'小茶是QQ 22222（茶茶）',tier:'foundation',category:'alias',subject:'22222',keys:['小茶','茶茶'],evidence:[{id:'source',quote:'小茶是QQ 22222（茶茶）'}]};
test('layering, evidence, correction, clutter, scope and failed persistence',async()=>{
 setup();const first=await save.execute(fact,exec);
 assert.equal((await save.execute(fact,exec)).id,first.id);
 await assert.rejects(save.execute({...fact,subject:'11111'},exec),/QQ/);
 await assert.rejects(save.execute({...fact,evidence:[{id:'fake',quote:'小茶是QQ 22222（茶茶）'}]},exec),/Evidence/);
 for(let n=0;n<10020;n++)db.set('clutter'+n,{id:'clutter'+n,title:'波动时长'+n,content:'旧统计',tags:[],importance:5,updated_at:'999999'});
 setup();const context=await foundationalContext(exec,'小茶是谁？');assert(context.includes('22222'));assert(!context.includes('旧统计'));assert(context.length<4500);
 const found=await search.execute({query:'小茶'},exec);assert.equal(found.items[0].tier,'foundation');assert.equal(found.items[0].subject,'22222');
 setup('纠正，小茶是QQ 33333（茶茶）');
 const corrected={...fact,subject:'33333',content:'小茶是QQ 33333（茶茶）',evidence:[{id:'source',quote:'纠正，小茶是QQ 33333（茶茶）'}],supersedes:[first.id]};
 const second=await save.execute(corrected,exec);assert(db.get(first.id).forgotten);assert((await foundationalContext(exec,'小茶')).includes('33333'));assert(!(await foundationalContext(exec,'小茶')).includes('22222'));
 await forget.execute({id:second.id},exec);assert(!(await foundationalContext(exec,'小茶')).includes('33333'));
 setup();approved=false;const beforeReject=db.size;assert.equal((await saveSummary([fact],exec))[0].action,'skipped');assert.equal(db.size,beforeReject);assert.equal((await saveSummary([{title:'行程',content:'自称去旁听，未经证实',tier:'episode'}],exec))[0].action,'skipped');assert.equal(db.size,beforeReject);await assert.rejects(save.execute({title:'错读反话',content:'要去旅游',tier:'episode'},exec),/暂不保存/);approved=true;
 setup();const episode=await save.execute({...fact,tier:'episode'},exec);assert.equal(unpack(db.get(episode.id).content).evidence[0].quote,fact.evidence[0].quote);fail=true;await assert.rejects(saveSummary([{title:'new',content:'new',tier:'topic'}],exec),/disk unavailable/);fail=false;
 configureMemory({events:()=>[{seq:1,type:'user/message',data:{source:{kind:'plugin'},content:[{type:'text',text:'旁听秘密'}]}},{seq:2,type:'tool/call',data:{name:'memory_search',callId:'m'}},{seq:3,type:'tool/result',data:{message:{content:[{type:'tool-result',toolCallId:'m',content:[{type:'text',text:'旧错误'}]}]}}}]});assert.deepEqual(sources(),[]);
});

test('failed Python output cannot be memory evidence',()=>{
 configureMemory({events:()=>[
  {seq:1,type:'tool/call',data:{name:'execute_python',callId:'p',arguments:'print(fake)'}},
  {seq:2,type:'tool/result',data:{message:{content:[{type:'tool-result',toolCallId:'p',content:[{type:'text',text:JSON.stringify({exit_code:1,output:'unverified partial data'})}]}]}}}
 ]});assert.deepEqual(sources(),[]);
});

test('evidence may omit id but must still quote a real source verbatim',async()=>{
 // The evidence index changes on every tool step, so the model no longer has to
 // copy an id: a verbatim quote is matched against this turn's authoritative
 // sources and the real source id is bound by the policy layer.
 setup('阿岚是QQ 55555（岚岚）');
 const quote={title:'阿岚',content:'阿岚是QQ 55555（岚岚）',tier:'foundation',category:'alias',subject:'55555',keys:['阿岚','岚岚'],evidence:[{quote:'阿岚是QQ 55555（岚岚）'}]};
 const saved=await save.execute(quote,exec);
 const stored=unpack(db.get(saved.id).content);
 assert.equal(stored.evidence[0].id,'source');
 assert.equal(stored.evidence[0].kind,'user');
 assert.equal(stored.evidence[0].quote,'阿岚是QQ 55555（岚岚）');
 // A quote nobody said in this turn is still refused.
 await assert.rejects(save.execute({...quote,title:'编造的阿岚',content:'阿岚还叫阿岚岚',evidence:[{quote:'这句原文本轮不存在'}]},exec),/Evidence/);
 // An explicit id must still name a real source; a fabricated id never passes.
 await assert.rejects(save.execute({...quote,title:'伪造的阿岚',evidence:[{id:'fake',quote:'阿岚是QQ 55555（岚岚）'}]},exec),/Evidence/);
 // An over-long or missing quote keeps the strict shape check.
 await assert.rejects(save.execute({...quote,title:'缺引用的阿岚',evidence:[{id:'source'}]},exec),/Evidence/);
});
