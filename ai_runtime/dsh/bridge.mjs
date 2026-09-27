import {createUserMessage, isAgentLoopRequest} from '@deepseek-ai/dsh-llm';
import {createInterface} from 'node:readline';
import {realpathSync, statSync} from 'node:fs';
import {resolve, sep} from 'node:path';
import {randomUUID} from 'node:crypto';
import {flushMemory} from './mneme.mjs';
import {prepareMemorySummary} from './memory-policy.mjs';

export const name = 'csbot-bridge';
export const inject = ['agents', 'sessionPersistence', 'compaction', 'tools', 'systemPrompt'];
const emit = value => process.stdout.write('CSBOT:' + JSON.stringify(value) + '\n');

export function apply(ctx) {
  const pending = new Map();
  const input = createInterface({input: process.stdin});
  let start;
  let liveAgent, accepting=false, hadSteer=false;
  const request = new Promise(resolve => {start = resolve;});
  input.on('line', line => {
    try {
      const value = JSON.parse(line);
      if (value.type === 'start') start(value);
      else if(value.type==='steer') {
        const accepted=Boolean(liveAgent && accepting);
        if(accepted) {hadSteer=true;liveAgent.steer(createUserMessage({content:[{type:'text',text:value.text}],source:{kind:'user'}}));}
        emit({type:'steer_ack',id:value.id,accepted});
      }
      else if (value.type === 'rpc_result') {
        const waiter = pending.get(value.id);
        if (waiter) {pending.delete(value.id); waiter(value);}
      }
    } catch { emit({type: 'protocol_error'}); }
  });
  (async () => {
    await ctx.get('loader')?.await();
    emit({type:'ready'});
    const req = await request;
    let trace=[];
    const flushTrace=()=>{if(trace.length){emit({type:'trace',events:trace});trace=[];}};
    const traceTimer=setInterval(flushTrace,80);
    const record=value=>{trace.push(value);if(trace.length>=64)flushTrace();};
    ctx.on('agent/assistant-stream',({agent,frame})=>{
      if(agent.session.id!==req.sessionId)return;
      if(req.guard && frame.type==='chunk' && (frame.chunk.type==='reasoning-delta' || (frame.chunk.type==='block-end' && frame.chunk.block.type==='reasoning')))return;
      // Native frames identify attempts, so retries replace rather than concatenate.
      if(frame.type!=='chunk' || ['text-delta','reasoning-delta','block-end','finish'].includes(frame.chunk.type))
        record({type:'assistant_stream',frame});
    });
    ctx.on('session/event',(session,event)=>{
      if(session.id!==req.sessionId)return;
      if(['tool/call','tool/result'].includes(event.type))record({type:event.type,data:event.data,time:event.time});
    });
    async function rpc(method, data) {
      const id=randomUUID();
      const response=new Promise(resolve=>pending.set(id,resolve));
      emit({type:'rpc',id,method,...data});
      const reply=await response;
      if (reply.error) throw new Error(reply.error);
      return reply;
    }
    // Guard the final provider response before DSH records it and before
    // Mneme's turn-end listener sees it. One-shot memory calls are excluded.
    ctx.on('llm/stream', async function* (options,next) {
      prepareMemorySummary(options);
      if (!req.guard || !isAgentLoopRequest(options)) {yield* next(); return;}
      const chunks=[];
      const attempt=randomUUID();
      for await (const chunk of next()) {
        chunks.push(chunk);
        if(chunk.type==='reasoning-delta')record({type:'reasoning_delta',attempt,text:chunk.text});
      }
      const blocks=chunks.filter(chunk=>chunk.type==='block-end').map(chunk=>chunk.block);
      const draft=blocks.filter(block=>block.type==='text').map(block=>block.text).join('\n');
      const terminal=chunks.at(-1);
      if (!draft || blocks.some(block=>block.type==='tool-call') || terminal?.reason?.kind!=='stop') {
        yield* chunks; return;
      }
      const reply=await rpc('guard_output',{text:draft});
      if (reply.result===draft) {yield* chunks; return;}
      yield {type:'block-start',index:0,blockType:'text'};
      yield {type:'text-delta',index:0,text:reply.result};
      yield {type:'block-end',index:0,block:{type:'text',text:reply.result}};
      for (const chunk of chunks) if (chunk.type==='usage') yield chunk;
      yield {type:'finish',reason:terminal.reason};
    });
    const root = realpathSync(process.env.CSBOT_WORKSPACE);
    const allowedFiles = new Set((req.readPaths ?? []).map(path => realpathSync(path)));
    const allow = new Set(['read', 'read_image', 'execute_python', 'memory_search', 'memory_save', 'memory_forget']);
    let toolCount = 0;
    ctx.tools.guard(exec => {
      if (!allow.has(exec.name)) return 'This tool is not enabled in CSBot.';
      if (++toolCount > 40) return 'Tool budget exhausted. Answer with available evidence.';
      if (exec.name === 'read' || exec.name === 'read_image') {
        try {
          const path = realpathSync(resolve(root, exec.arguments.file_path));
          if (!(path === root || path.startsWith(root + sep) || allowedFiles.has(path))) return 'Path is outside the authorized read-only workspace.';
          if (statSync(path).size > 32*1024**2) return 'File exceeds read limit.';
        } catch {return 'File is unavailable; an original image may have been evicted. Check its current status.';}
      }
    });
    ctx.tools.register({
      name:'execute_python',
      description:'Run Python in a disposable isolated container (45s, 192 MiB, no internet). Use import csdata; csdata.catalog(), csdata.call(name, **params), csdata.query(sql, params, source="main"|"game"), csdata.search(query), csdata.block(id), csdata.image(id). Read DATA.md first. Print concise results. csdata.artifact(path) exports a small result file; csdata.artifact("chart.png", send=True, caption="...") submits an image to the user with this reply.',
      parameters:{type:'object',properties:{code:{type:'string'}},required:['code'],additionalProperties:false},
      output:{schema:{type:'object'},render:(_,value)=>[{type:'text',text:JSON.stringify(value)}]},
      async execute(args, exec) {
        const reply = await rpc('execute_python',{code:args.code});
        for (const path of reply.readPaths ?? []) {
          try {allowedFiles.add(realpathSync(path));} catch {}
        }
        return reply.result;
      },
    });
    const options={provider:'deepseek-official',model:process.env.CSBOT_MODEL,maxTokens:8192,
                   reasoningEffort:process.env.CSBOT_THINKING === '1' ? 'high' : 'off'};
    const setup = agentCtx => {
      // Restriction also removes unused tools from the model schema.
      const available=ctx.tools.schemas().map(tool=>tool.name).filter(name=>allow.has(name));
      agentCtx.tools.restrict({allow:available});
    };
    const exists = await ctx.sessionPersistence.stat(req.sessionId);
    const handle = exists
      ? await ctx.agents.resume({resumeSessionId:req.sessionId,agentOptions:options,setup})
      : await ctx.agents.create({sessionId:req.sessionId,meta:{cwd:root},agentOptions:options,setup});
    liveAgent=handle.agent;accepting=true;
    if (req.legacyMemory) {
      // Import previously explicit group memory faithfully, without another
      // model rewrite. Stable titles make a crash/retry idempotent.
      const characters=Array.from(req.legacyMemory);
      const parts=[];for(let i=0;i<characters.length;i+=4000)parts.push(characters.slice(i,i+4000).join(''));
      for (let index=0;index<parts.length;index++) {
        const result=await ctx.tools.execute({callId:randomUUID(),name:'memory_save',
          arguments:{type:'history',title:`旧群记忆迁移 ${index+1}`,content:parts[index],
                     source:'legacy-ai-mem-explicit',importance:4},agent:handle.agent,signal:new AbortController().signal});
        if (result.isError) throw new Error('Legacy memory import failed');
      }
    }
    if(req.memoryOnly) {
      accepting=false;
      const repaired=[];
      for(const action of req.memoryActions ?? []) {
        if(!['memory_save','memory_forget'].includes(action.name))throw new Error('Unsupported offline memory action');
        const result=await ctx.tools.execute({callId:randomUUID(),name:action.name,arguments:action.arguments,agent:handle.agent,signal:new AbortController().signal});
        if(result.isError)throw new Error('Offline memory action failed');
        repaired.push({name:action.name,value:result.value});
      }
      let verified=0;
      for(const title of req.memoryCheckTitles ?? []) {
        const result=await ctx.tools.execute({callId:randomUUID(),name:'memory_search',
          arguments:{query:title,mode:'keyword',limit:100},agent:handle.agent,signal:new AbortController().signal});
        if(result.isError || !result.value?.items?.some(item=>item.title===title))throw new Error('Imported memory is not searchable');
        verified++;
      }
      await flushMemory();await ctx.sessionPersistence.flush();
      clearInterval(traceTimer);trace=[];
      await handle.dispose();
      emit({type:'complete',reason:{kind:'completed'},messages:[],tools:[],verified,repaired});
      input.close();process.exit(0);
    }
    const before = handle.agent.session.snapshotEvents().length;
    if (req.context) handle.agent.inject(createUserMessage({content:[{type:'text',text:req.context}],source:{kind:'plugin',plugin:'csbot-context'}}));
    handle.agent.followup(createUserMessage({content:[{type:'text',text:req.text}],source:{kind:req.remember ? 'user' : 'plugin',...(!req.remember ? {plugin:'csbot-report'} : {})}}));
    await handle.agent.whenIdle();
    accepting=false;
    await flushMemory();
    const compaction = req.compact ? await ctx.compaction.compactNow(handle.agent,new AbortController().signal) : null;
    await ctx.sessionPersistence.flush();
    const events=handle.agent.session.snapshotEvents().slice(before);
    const end=events.filter(event=>event.type==='turn/end').at(-1);
    const messages=events.filter(event=>event.type==='assistant/message' && !event.data.message.content.some(block=>block.type==='tool-call')).slice(hadSteer?0:-1).map(event=>({
      role:'assistant',content:event.data.message.content.filter(block=>block.type==='text').map(block=>block.text).join('\n'),
    })).filter(message=>message.content);
    const toolEvents=events.filter(event=>event.type==='tool/call'||event.type==='tool/result').map(event=>({type:event.type,data:event.data}));
    clearInterval(traceTimer);flushTrace();
    await handle.dispose();
    emit({type:'complete',reason:end?.data.reason,messages,tools:toolEvents,resumed:Boolean(exists),compacted:Boolean(compaction)});
    input.close();
    process.exit(0);
  })().catch(error=>{emit({type:'error',error:String(error.message)});process.exit(1);});
}
