#!/usr/bin/env python
# -*- coding:utf-8 -*-
from task.search.cracker.gsxt_guizhou_worker import GsxtGuiZhouWorker


class GsxtSearchListGuiZhouWorker(GsxtGuiZhouWorker):
    def __init__(self, **kwargs):
        GsxtGuiZhouWorker.__init__(self, **kwargs)

    def get_detail_html_list(self, seed, session, param_list):
        return len(param_list), []
