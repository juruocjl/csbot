// DSH 0.1.5 publishes session/event observers without awaiting their promises.
// Keep the upstream Mneme lifecycle, but drain its turn-end writes before exit.
import * as mneme from '@modusensus/dsh-mneme';
import {wrapMemoryTool} from './layered-memory.mjs';
export const {name, inject, Config} = mneme;
const pending = new Set();
export async function flushMemory() {
  while (pending.size) await Promise.all([...pending]);
}
export const apply = (ctx, config) => {
 const tools = new Proxy(ctx.tools,{get(target,key){if(key==='register')return tool=>target.register(wrapMemoryTool(tool)); const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;}});
 return mneme.apply(new Proxy(ctx, {
  get(target, property) {
    if(property==='tools')return tools;
    if(property==='inject')return (deps,callback,...rest)=>target.inject(deps,child=>callback(new Proxy(child,{get(c,k){return k==='tools'?tools:Reflect.get(c,k,c);}})),...rest);
    if (property !== 'on') return Reflect.get(target, property, target);
    return (event, callback, ...options) => target.on(event, event === 'session/event'
      ? function (...args) {
          const result = callback.apply(this, args);
          if (result?.then) {
            const task = Promise.resolve(result);
            pending.add(task);
            task.then(() => pending.delete(task), () => pending.delete(task));
          }
          return result;
        }
      : callback, ...options);
  },
}), config);
};
