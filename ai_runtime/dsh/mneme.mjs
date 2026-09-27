// DSH 0.1.5 publishes session/event observers without awaiting their promises.
// Keep the upstream Mneme lifecycle, but drain its turn-end writes before exit.
import * as mneme from '@modusensus/dsh-mneme';
export const {name, inject, Config} = mneme;
const pending = new Set();
export async function flushMemory() {
  while (pending.size) await Promise.all([...pending]);
}
export const apply = (ctx, config) => mneme.apply(new Proxy(ctx, {
  get(target, property) {
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
