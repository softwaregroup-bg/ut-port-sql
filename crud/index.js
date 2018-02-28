const crud = {
    create: require('./create'),
    read: require('./read'),
    update: require('./update'),
    delete: require('./delete')
};

module.exports = {
    actions: Object.keys(crud),
    generate: (binding, action) => {
        let name;
        let suffix;
        if (binding.name.match(/]$/)) {
            name = binding.name.slice(0, -1);
            suffix = ']';
        } else {
            name = binding.name;
            suffix = '';
        }
        binding.spName = `${name}.${action}${suffix}`;
        binding.tt = `${name}TT${suffix}`;
        binding.ttu = `${name}TTU${suffix}`;
        return crud[action](binding);
    }
};
