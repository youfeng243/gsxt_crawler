#!/usr/bin/python2.6
#coding=utf8

import logging, logging.handlers, datetime, re

def str_dict(struct):
    if isinstance(struct, dict):
        for k, v in struct.items():
            del struct[k]
            struct[str(k)] = str(v)
    elif hasattr(struct, '__dict__'):
        attr_names = struct.__dict__.keys()
        for name in attr_names:
            sub_struct = getattr(struct, name)
            str_dict(sub_struct)
    elif isinstance(struct, (list, tuple)):
        for sub_struct in struct:
            str_dict(sub_struct)

