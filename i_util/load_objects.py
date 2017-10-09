# -*- coding:utf-8 -*-
from importlib import import_module
from pkgutil import iter_modules

def load_object(path, prefix='', suffix=''):
    cls_dict = {}
    mods = walk_modules(path)
    for mod in mods:
        if hasattr(mod, '__path__'):
            continue
        mod_name = mod.__name__
        mod_name = mod_name.split('.')[-1]
        for key, cls in vars(mod).items():
            if key.lower() == prefix + mod_name.lower() + suffix:
                cls_dict[mod_name] = cls
                break
    return cls_dict

def walk_modules(path):
    """Loads a module and all its submodules from a the given module path and
    returns them. If *any* module throws an exception while importing, that
    exception is thrown back.

    For example: walk_modules('scrapy.utils')
    """

    mods = []
    mod = import_module(path)
    mods.append(mod)
    if hasattr(mod, '__path__'):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = path + '.' + subpath
            if ispkg:
                mods += walk_modules(fullpath)
            else:
                submod = import_module(fullpath)
                mods.append(submod)
    return mods
