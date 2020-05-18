from .common import EMPTY, TYPE_FORMAT_MAPPERS


def _group(seq, args):
    ops = args[-1]
    acc = {}

    for record in seq:
        acc_key = ''

        for ind in range(0, len(args) - 1):
            val = record.get(args[ind], EMPTY)

            if val != EMPTY:
                acc_key += TYPE_FORMAT_MAPPERS[type(val)](val)

        if acc_key not in acc:
            acc[acc_key] = dict(record)

        current_acc = acc[acc_key]

        for op_key, op_val in ops.items():
            if op_key == '$count':
                current_acc[op_val] = current_acc.get(op_val, 0) + 1
            elif op_key == '$last':
                for key in op_val:
                    current_acc[key] = record.get(key, None)
            elif op_key == '$mul':
                current_acc[op_val[1]] = current_acc.get(op_val[1], 1) * record.get(op_val[0], 0)
            elif op_key == '$push':
                val = record.get(op_val[0], EMPTY)

                if val != EMPTY:
                    if op_val[1] not in current_acc:
                        current_acc[op_val[1]] = [val]
                    else:
                        current_acc[op_val[1]].append(val)
            elif op_key == '$sum':
                current_acc[op_val[1]] = current_acc.get(op_val[1], 0) + record.get(op_val[0], 0)

    for rec in acc.values():
        yield rec


def _o2m(seq, args):
    key_from, key_to, key_as, db = args

    for record in seq:
        val_from = record.get(key_from, EMPTY)

        if val_from != EMPTY:
            record[key_as] = list(db.select({key_to: val_from}))

        yield record


def _o2o(seq, args):
    key_from, key_to, key_as, db = args

    for record in seq:
        val_from = record.get(key_from, EMPTY)

        if val_from != EMPTY:
            record[key_as] = db.select_one({key_to: record.get})

        yield record


def _override(seq, key):
    for record in seq:
        val = record.get(key, EMPTY)

        if val != EMPTY and isinstance(val, dict):
            del record[key]

            record.update(val)

        yield record


def _project(seq, keys):
    for record in seq:
        for key_from, key_to in keys.items():
            val = record.get(key_from, EMPTY)

            if val != EMPTY:
                del record[key_from]

                record[key_to] = val

            yield record


def _unwind(seq, key):
    for record in seq:
        val = record.get(key, EMPTY)

        if val != EMPTY:
            if isinstance(val, list):
                if len(val):
                    for subrec in record[key]:
                        unwind = dict(record)
                        unwind[key] = subrec
                    
                        yield unwind
                    
                    continue
                else:
                    del record[key]

        yield record


_AGGREGATORS = {
    '$group': _group,
    '$o2o': _o2o,
    '$o2m': _o2m,
    '$override': _override,
    '$project': _project,
    '$unwind': _unwind,
}


def aggregate(seq, *pipes):
    for pipe, args in pipes:
        if not callable(pipe):
            pipe = _AGGREGATORS.get(pipe, None)

        if pipe:
            seq = pipe(seq, args)

    return seq
