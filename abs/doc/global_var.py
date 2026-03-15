# -*- coding: utf-8 -*-

"""全局变量管理

"""

class global_var():
    GLOBALS_DICT = {}

    def set(self, name, value):
        try:
            self.GLOBALS_DICT[name] = value
            return True
        except KeyError:
            return False


    def get(self, name):
        try:
            return self.GLOBALS_DICT[name]
        except KeyError:
            return "Not found"