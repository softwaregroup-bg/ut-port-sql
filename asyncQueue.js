const noop = x => x;
module.exports = ({
    concurrency = 1,
    bail = true,
    onError = x => x
} = {}) => {
    let running = 0;
    const tasks = [];

    const run = async task => {
        running++;
        try {
            await task();
        } catch (e) {
            try {
                await onError(e);
            } finally {
                if (bail) {
                    tasks.splice(0, tasks.length);
                    api.push = api.wrap = noop;
                }
            }
        }
        running--;
        if (tasks.length > 0) run(tasks.shift());
    };

    const push = task => running < concurrency ? run(task) : tasks.push(task);

    const wrap = fn => (...params) => push(() => fn(...params));

    const api = {
        push,
        wrap
    };

    return api;
};
