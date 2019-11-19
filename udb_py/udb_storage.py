class UdbStorage(object):
    def is_available(self):
        raise NotImplementedError

    def is_capture_events(self):
        return False

    def drop(self):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError

    def save(self, indexes, revision, data):
        raise NotImplementedError

    def save_meta(self, indexes, revision):
        raise NotImplementedError

    def on_delete(self, rid):
        return self

    def on_insert(self, rid, record):
        return self

    def on_update(self, rid, record, values):
        return self
