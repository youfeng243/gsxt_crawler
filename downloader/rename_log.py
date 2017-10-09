#!/usr/bin/env python
# coding=utf8


import os


def control(file_path):
    list_file = os.listdir(file_path)

    for i in list_file:
        if i.find('.COMPLETED') > 0:
            old_name = os.path.abspath(i)
            new_name = old_name[:-10]
            print old_name
            print new_name
            os.rename(old_name, new_name)


if __name__ == "__main__":
    path = '../logs'
    control(path)
