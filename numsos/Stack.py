from __future__ import print_function
import numpy as np
from sosdb.DataSet import DataSet
import textwrap

class Stack(object):
    def __init__(self):
        self.stack = []

    def push(self, o):
        """Push the argument to the top of the stack"""
        self.stack.append(o)
        return o

    def dup(self):
        """Push the value on the top of the stack"""
        self.stack.append(self.stack[-1])
        return self.top()

    def pop(self):
        """Remove the top of the stack and return it's value"""
        try:
            o = self.stack.pop()
            return o
        except:
            raise IndexError("The stack is empty.")

    def drop(self):
        """Drop the element at the top of the stack"""
        self.stack.pop()
        return None

    def pick(self, n):
        """Return the n-th element from the top of the stack

        Return TOP-n from the stack. The items is note removed from
        the stack.
        """
        return self.stack[-1 - n]

    def __len__(self):
        return len(self.stack)

    def is_empty(self):
        """Return True if the stack is empty, otherwise return False"""
        return len(self.stack) == 0

    def swap(self):
        """Swap the top two elements of the stack"""
        a = self.stack.pop()
        b = self.stack.pop()
        self.stack.append(a)
        self.stack.append(b)
        return self.top()

    def top(self):
        """Return the top of the stack"""
        return self.stack[-1]

    def show(self):
        """Show the contents of the stack"""
        j = 0
        for e in reversed(self.stack): # range(len(self.stack)-1, -1, -1):
            if j == 0:
                print("[TOP] ".format(j), end='')
            else:
                print("[{0:3}] ".format(j), end='')
            skip = 1
            if type(e) == DataSet:
                print("{0:4} ".format(e.get_series_size()), end='')
                rows = textwrap.wrap(str(e.series), 80 - 12)
                for row in rows:
                    print("{0:{width}}{1}".format("", row, width=skip))
                    skip = 12
            else:
                print("{0} ".format(e))
            j += 1
