import os.path as op
import os
import json

class BaseStateStore(object):
    def __init__(self, meta_configs, appname):
        self._meta_configs = meta_configs
        self._appname = appname

    def update_state(self, key, states):
        pass

    def get_state(self, key):
        pass

    def delete_state(self, key):
        pass


class FileStateStore(BaseStateStore):
    def __init__(self, meta_configs, appname):
        """
        :meta_configs: dict like and contains checkpoint_dir, session_key,
        server_uri etc
        """

        super(FileStateStore, self).__init__(meta_configs, appname)

    def update_state(self, key, states):
        """
        :state: Any JSON serializable
        :return: None if successful, otherwise throws exception
        """

        fname = op.join(self._meta_configs["checkpoint_dir"], key)
        with open(fname + ".new", "w") as jsonfile:
            json.dump(states, jsonfile)

        if op.exists(fname):
            os.remove(fname)

        os.rename(fname + ".new", fname)
        # commented this to disable state cache for local file
        # if key not in self._states_cache:
        # self._states_cache[key] = {}
        # self._states_cache[key] = states

    def get_state(self, key):
        fname = op.join(self._meta_configs["checkpoint_dir"], key)
        if op.exists(fname):
            with open(fname) as jsonfile:
                state = json.load(jsonfile)
                # commented this to disable state cache for local file
                # self._states_cache[key] = state
                return state
        else:
            return None
