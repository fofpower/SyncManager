import threading
from smyt_sync_manager.config import settings
from smyt_sync_manager.sync_manager.sync_helper import update_by_keys, delete_by_keys, save_log


class ThreadHandler:
    def __init__(self, jobs, results, status):
        self.jobs = jobs
        self.results = results
        self.status = status

    def create_threads(self):
        for _ in range(settings.CONCURRENCY):
            thread = threading.Thread(target=self.worker)
            thread.daemon = True
            thread.start()

    def worker(self):
        while True:
            try:
                param = self.jobs.get()
                result = self.task(param)
                self.results.put(result)
            finally:
                self.jobs.task_done()

    def task(self, *args, **kwargs):
        return

    def add_jobs(self, *args, **kwargs):
        for i in range(10):
            if not self.status.empty():
                # self.jobs.queue.clear()
                break
            self.jobs.put(i)

    def process(self):
        self.jobs.join()
        done = 0
        while not self.results.empty():
            done += self.results.get()
        status = None
        if not self.status.empty():
            status = self.status.get()
        return done, status


class UpdateByKey(ThreadHandler):
    def __init__(self, jobs, results, status, table_name, key_columns, source_engine, local_engine):
        super().__init__(jobs, results, status)
        self.table = table_name
        self.key_columns = key_columns
        self.source_engine = source_engine
        self.local_engine = local_engine

    def task(self, key_values):
        try:
            return update_by_keys(self.table, self.key_columns, key_values, self.source_engine, self.local_engine)
        except Exception as e:
            error = str(e)
            save_log(self.table, 0, 'Error occur while updating: {}'.format(error), 'UPDATE-0', self.local_engine)
            self.status.put(error)
            return 0

    def add_jobs(self, key_values_generator):
        for key_values in key_values_generator:
            if not self.status.empty():
                # self.jobs.queue.clear()
                break
            self.jobs.put(key_values)


class DeleteByKey(ThreadHandler):
    def __init__(self, jobs, results, status, table_name, key_columns, local_engine):
        super().__init__(jobs, results, status)
        self.table = table_name
        self.key_columns = key_columns
        self.local_engine = local_engine

    def task(self, key_values):
        try:
            return delete_by_keys(self.table, self.key_columns, key_values, self.local_engine)
        except Exception as e:
            error = str(e)
            save_log(self.table, 0, 'Error occur while remove deleted records: {}'.format(error), 'DELETE-0',
                     self.local_engine)
            self.status.put(error)
            return 0

    def add_jobs(self, key_values_generator):
        for key_values in key_values_generator:
            if not self.status.empty():
                # self.jobs.queue.clear()
                break
            self.jobs.put(key_values[0])


class CompareByKey(ThreadHandler):
    def __init__(self, jobs, results, status, table_name, key_columns, source_engine, local_engine):
        super().__init__(jobs, results, status)
        self.table = table_name
        self.key_columns = key_columns
        self.local_engine = local_engine
        self.source_engine = source_engine

    def task(self, key_values):
        try:
            deleted = delete_by_keys(self.table, self.key_columns, key_values[0], self.local_engine)
            updated = update_by_keys(self.table, self.key_columns, key_values[1], self.source_engine, self.local_engine)
            return {"updated": updated, 'deleted': deleted}
        except Exception as e:
            error = str(e)
            save_log(self.table, 0, 'Error occur while check: {}'.format(error), 'CHECK-0', self.local_engine)
            self.status.put(error)
            return {"updated": 0, 'deleted': 0}

    def add_jobs(self, key_values_generator):
        for key_values in key_values_generator:
            if not self.status.empty():
                # self.jobs.queue.clear()
                break
            self.jobs.put(key_values)

    def process(self):
        self.jobs.join()
        updated, deleted = 0, 0
        while not self.results.empty():
            result = self.results.get()
            updated += result.get('updated', 0)
            deleted += result.get('deleted', 0)
        status = None
        if not self.status.empty():
            status = self.status.get()
        return updated, deleted, status
