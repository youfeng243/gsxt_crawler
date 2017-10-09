import cProfile
import pstats
import uuid


def singleton(cls, *args, **kw):
    instance = {}

    def _singleton(*args, **kw):
        # double check singleton
        if cls in instance: return instance[cls]
        with cls.singleton_lock:
            if cls not in instance:
                instance[cls] = cls(*args, **kw)
            return instance[cls]

    return _singleton


@singleton
class ProfileGlobalSettings:
    import threading
    singleton_lock = threading.Lock()
    enable_profile = False

    def __init__(self):
        pass

    def set_enabled(self, enable):
        self.enable_profile = enable

    def is_enabled(self):
        return self.enable_profile


class ZProfiler:
    global_settings = ProfileGlobalSettings()
    profiler = None
    this_started = False

    def __init__(self, prof):
        self.profiler = prof

    # You can turn on sampling only when enabled globaly
    def begin(self):
        if self.global_settings.is_enabled():
            if not self.is_this_started():
                self.this_started = True
                self.profiler.enable()

    # You can turn off sampling only if this profiler is started
    def end(self):
        if self.is_this_started():
            self.profiler.disable()
            self.this_started = False

    def is_this_started(self):
        return self.this_started


profiler_list = []
settings = ProfileGlobalSettings()


def profiler_creator(*args):
    profiler = cProfile.Profile()
    profiler_list.append(profiler)
    return ZProfiler(profiler)


def merge_profile_res():
    global profiler_list
    if len(profiler_list) < 1:
        raise Exception("Profilers not initialized")
    for profiler in profiler_list:
        profiler.create_stats()
    final_stat = None
    profiler_list_dump = filter(lambda p: len(p.stats) > 0, profiler_list)
    stats = map(lambda p: pstats.Stats(p), profiler_list_dump)
    if len(stats) > 0:
        final_stat = reduce(lambda p1, p2: p1.add(p2), stats)
    return final_stat


def print_global_profile_res():
    stat = merge_profile_res()
    if stat is not None:
        stat.print_stats()
    else:
        print "Profiling data is empty"


def dump_global_profile_res(name):
    path = name + ".prof." + str(uuid.uuid4())
    stat = merge_profile_res()
    if stat is not None:
        print "Writing profiling result to " + path
        stat.dump_stats(path)
    else:
        of = open(path, "w")
        of.write("Profiling data is empty")
        of.close()


def global_enable():
    global settings
    settings.set_enabled(True)


def global_disable():
    global settings
    settings.set_enabled(False)


def global_enabled():
    global settings
    return settings.is_enabled()


def global_clear():
    global profiler_list
    for profiler in profiler_list:
        profiler.clear()


def profiling_signal_handler(name, a, b):
    if not global_enabled():
        global_clear()
        global_enable()
    else:
        global_disable()
        dump_global_profile_res(name)
