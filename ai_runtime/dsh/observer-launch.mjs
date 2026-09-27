import {spawn} from 'node:child_process';
import {mkdirSync,writeFileSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createInterface} from 'node:readline';
const root=dirname(fileURLToPath(import.meta.url));
const home=process.env.DSH_HOME;
const host=process.env.CSBOT_DSH_WEB_HOST;
const octets=(host??'').split('.').map(Number);
if(octets.length!==4 || octets.some(x=>!Number.isInteger(x)||x<0||x>255) || octets[0]!==100 || octets[1]<64 || octets[1]>127)
 throw new Error('CSBOT_DSH_WEB_HOST must be an IPv4 Tailscale address');
if(!home) throw new Error('DSH_HOME required');
mkdirSync(home,{recursive:true,mode:0o700});
const patch=join(home,'observer.json');
writeFileSync(patch,JSON.stringify([
 {id:'session-persistence-jsonl',config:{root:process.env.CSBOT_DSH_SESSIONS,compression:'none'}},
 {id:'session-log-deepseek',disabled:true},
 {insert:[{id:'csbot-observer',name:join(root,'observer.mjs'),config:{}}]},
]),{mode:0o600});
const child=spawn(process.execPath,[join(root,'node_modules/@deepseek-ai/dsh/lib/bin.js'),'--profile','web','--patch',patch,
 '--no-open','--host','127.0.0.1','--port',process.env.CSBOT_DSH_INTERNAL_PORT??'3089','--trusted-host',host+':'+(process.env.CSBOT_DSH_WEB_PORT??'3088')],{stdio:['ignore','pipe','pipe'],env:{...process.env,HOME:home}});
let ready=false;
const timer=setTimeout(()=>{console.error('Observer startup timed out');child.kill('SIGTERM');},90000);
createInterface({input:child.stdout}).on('line',line=>{
 if(line.startsWith('dsh web:')) {
  const url=new URL(line.slice('dsh web:'.length).trim());url.hostname=host;url.port=process.env.CSBOT_DSH_WEB_PORT??'3088';
  writeFileSync(join(home,'login-url'),url.href+'\n',{mode:0o600});
  ready=true;clearTimeout(timer);console.log('DSH read-only observer ready at '+url.origin);
 }
 // Never copy authenticated launch URLs or chat content to the journal.
});
child.stderr.on('data',chunk=>process.stderr.write(chunk.toString().replace(/https?:\/\/\S+/g,'[URL]')));
for(const signal of ['SIGTERM','SIGINT']) process.on(signal,()=>child.kill(signal));
child.on('exit',(code)=>{clearTimeout(timer);process.exit(code??(ready?0:1));});
