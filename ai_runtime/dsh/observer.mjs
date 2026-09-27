import {RemoteError} from '@deepseek-ai/dsh-typert-protocol';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
export const name='csbot-observer';
export const inject=['typertGateway','tools','webServer','llm'];
const allowed=new Set([
 'session/list','session/page','session/follow','session/control','session/modelCatalog',
 'session/canOpenWorkspacePath','session/attachment','settings/describe',
 'settings/canOpenAgentPresetDirectory','credentials/describe','workspace/follow',
]);
export function checkRead(request) {
 const key=request.namespace+'/'+request.method;
 if(!allowed.has(key)) throw new RemoteError('observer/read-only','此入口仅查看 CSBot 会话，不能发送、修改设置或执行操作。',{});
}
export function apply(ctx) {
 // Gate both unary RPC and WebSocket streams before any agent lookup/resume.
 for(const name of ['invoke','stream']) {
  const original=ctx.typertGateway[name].bind(ctx.typertGateway);
  ctx.typertGateway[name]=async request=>{
   checkRead(request);
   const result=await original(request);
   if(request.namespace==='session' && request.method==='list') {
    const manifest=JSON.parse(readFileSync(join(process.env.CSBOT_DSH_SESSIONS,'manifest.json'),'utf8'));
    for(const item of result.items) {
     const entry=manifest[item.sessionId];
     if(entry) item.projections={asOfSeq:entry.seq,values:{...item.projections?.values,title:entry.title}};
    }
   }
   return result;
  };
 }
 ctx.tools.guard(()=>'CSBot observer is read-only.');
 ctx.on('llm/stream',async function*(){throw new Error('Model execution is disabled in the observer');});
 ctx.webServer.tapIndex(html=>html.replace('<head>','<head><style>body::before{content:"CSBot · 只读轨迹 · 每 5 秒同步，刷新查看新记录";display:block;padding:6px 14px;background:#243449;color:#fff;font:13px sans-serif;position:fixed;bottom:0;right:0;z-index:99999;pointer-events:none}</style>'));
}
