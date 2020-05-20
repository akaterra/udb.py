from .common import cpy_dict, EMPTY, TYPE_FORMAT_MAPPERS


def _count_group_op(acc, key, record):
    acc[key] = acc.get(key, 0) + 1


def _last_group_op(acc, keys, record):
    for key in keys:
        acc[key] = record.get(key, None)


def _max_group_op(acc, args, record):
    if args[1] not in acc:
        acc[args[1]] = None

    val = record.get(args[0], EMPTY)

    if val != EMPTY:
        acc[args[1]] = max(val, acc[args[1]]) if acc[args[1]] is not None else val


def _min_group_op(acc, args, record):
    if args[1] not in acc:
        acc[args[1]] = None

    val = record.get(args[0], EMPTY)

    if val != EMPTY:
        acc[args[1]] = min(val, acc[args[1]]) if acc[args[1]] is not None else val


def _mul_group_op(acc, args, record):
    acc[args[1]] = acc.get(args[1], 1) * record.get(args[0], 0)


def _push_group_op(acc, args, record):
    if args[1] not in current_acc:
        acc[args[1]] = []

    val = record.get(args[0], EMPTY)

    if val != EMPTY:
        acc[args[1]].append(val)


def _sum_group_op(acc, args, record):
    acc[args[1]] = acc.get(args[1], 0) + record.get(args[0], 0)


_GROUP_OPS = {
    '$count': _count_group_op,
    '$last': _last_group_op,
    '$max': _max_group_op,
    '$min': _min_group_op,
    '$mul': _mul_group_op,
    '$push': _push_group_op,
    '$sum': _sum_group_op,
}


def _group(seq, args):
    ops = args[-1]
    acc = {}

    for record in seq:
        acc_key = ''

        for ind in range(0, len(args) - 1):
            val = record.get(args[ind], EMPTY)

            if val != EMPTY:
                acc_key += TYPE_FORMAT_MAPPERS[type(val)](val)
            else:
                acc_key += TYPE_FORMAT_MAPPERS[type(None)](None)

        if acc_key not in acc:
            acc[acc_key] = dict(record)

        current_acc = acc[acc_key]

        for op_key, op_val in ops.items():
            op = _GROUP_OPS.get(op_key, None)

            if op:
                op(current_acc, op_val, record)

    for rec in acc.values():
        yield rec


def _limit(seq, count):
    for val in seq:
        if count == 0:
            break
        
        count -= 1
        
        yield val


def _o2m(seq, args):
    key_from, key_to, key_as, db = args

    for record in seq:
        val_from = record.get(key_from, EMPTY)

        if val_from != EMPTY:
            record[key_as] = list(db.select({key_to: val_from}))

        yield record


def _o2o(seq, args):
    key_from, key_to, db, key_as = args

    for record in seq:
        val_from = record.get(key_from, EMPTY)

        if val_from != EMPTY:
            record[key_as] = db.select_one({key_to: val_from})

        yield record


def _offset(seq, count):
    for val in seq:
        if count > 0:
            count -= 1

            continue
        
        yield val


def _project(seq, keys):
    for record in seq:
        for key_from, key_to in keys.items():
            val = record.get(key_from, EMPTY)

            if val != EMPTY:
                del record[key_from]

                record[key_to] = val

            yield record


def _rebase(seq, args):
    key = None
    skip_exising = False

    if len(args) > 1:
        key, skip_exising = args[0], args[1]
    else:
        key = args

    if skip_exising:  # @todo
        for record in seq:
            val = record.get(key, EMPTY)

            if val != EMPTY and isinstance(val, dict):
                del record[key]

                record.update(val)

            yield record
    else:
        for record in seq:
            val = record.get(key, EMPTY)

            if val != EMPTY and isinstance(val, dict):
                del record[key]

                record.update(val)

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


_PIPES = {
    '$group': _group,
    '$limit': _limit,
    '$o2o': _o2o,
    '$o2m': _o2m,
    '$offset': _offset,
    '$project': _project,
    '$rebase': _rebase,
    '$unwind': _unwind,
}


def aggregate(seq, *pipes):
    for pipe, args in pipes:
        if not callable(pipe):
            pipe = _PIPES.get(pipe, None)

        if pipe:
            seq = pipe(seq, args)

    return seq


def register_aggregation_pipe(pipe, fn):
    _PIPES[pipe] = fn
