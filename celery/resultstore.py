from celery.backends.amqp import AMQPBackend
from celery.backends import default_backend


class ResultStore(AMQPBackend):

    def _get_entity(self, task_id, what, action):
        if task_id in self._cache:
            return self._cache[task_id][what]

        get_store = getattr(default_backend, action)
        in_store = get_store(task_id)
        if in_store != "PENDING":
            return in_store

        superself = super(ResultStore, self)
        get_super = getattr(superself, action)
        return get_super(task_id)

    def get_status(self, task_id)
        return self._get_entity(task_id, "status", "get_status")

    def get_result(self, task_id)
        return self._get_entity(task_id, "result", "get_result")

    def get_traceback(self, task_id)
        return self._get_entity(task_id, "traceback", "get_traceback")

    def _get_task_meta_for(self, task_id):
        if task_id in self._cache:
            return self._cache[task_id]

        task_meta = super(ResultStore, self)._get_task_meta_for(task_id)
        if task_meta:
            default_backend.store_result(**task_meta)
        return task_meta

result_store = ResultStore()
