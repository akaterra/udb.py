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

    def on_delete(self, id):
        return self

    def on_insert(self, id, record):
        return self

    def on_update(self, id, record, values):
        return self
