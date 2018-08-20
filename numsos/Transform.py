import numpy as np
from numsos.Stack import Stack
from numsos.DataSet import DataSet, ArrayDataByIndex, ListDataByIndex

class Transform(object):
    def __init__(self, dataSrc, dataSink, limit=1024):
        self.source = dataSrc
        self.sink = dataSink
        self.stack = Stack()
        self.window = limit
        self.keep = 0
        self.ops = {
            "+"    : self.add,
            "-"    : self.subtract,
            "*"    : self.multiply,
            "/"    : self.divide,
            '--'   : self.diff,
            'grad' : self.gradient,
            'hist' : self.histogram,
            'min' : self.min,
            'max' : self.max,
            'mean' : self.mean,
            'std'  : self.std
        }

    def _next(self, count=None, wait=None, keep=0, reset=True):
        if count:
            limit = count
        else:
            limit = self.window
        result = self.source.get_results(limit=limit, keep=keep, reset=reset, wait=wait)
        if result:
            return self.stack.push(result)
        return None

    def begin(self, count=None, wait=None):
        """Begin reading series the data source

        Keyword Parameters:
        count-- The maximum number of samples to return
        wait -- A tuple specifying a function and argument that is
                called if data is exhausted before count samples are
                available
        """
        return self._next(count=count, wait=wait, reset=True)

    def next(self, count=None, wait=None, keep=0):
        """Continue reading series from the data source

        Keyword Parameters:
        count-- The maximum number of samples to return
        wait -- A tuple specifying a function and argument that is
                called if data is exhausted before count samples are
                available
        keep -- The number of samples to retain from the previous call
                to next()
        """
        return self._next(count=count, wait=wait, keep=keep, reset=False)

    def row(self, row_no):

        """Return the row with the specified index

        The result returned will have a single selected row

        Postional Arguments:
        -- The row number 0 ... get_series_size()
        """
        inp = self.stack.pop()
        res = inp.new_set(DataSet.NUMERIC_DATA, 1, inp.series)

        for col in range(0, inp.get_series_count()):
            res[col][0] = inp[col][row_no]

        res.set_series_size(1)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def group(self, series_list, group_fn, count=None):
        """Filter a DataSet with a group function

        Pop the top of stack the return each row for which the
        group_fn function returns True

        If count is specified, it is the name of a series that will be
        inserted into the output that is the count of rows matching
        each group.

        The result pushed to the stack will contain N rows where N is
        the number of times the group_fn function returned True.

        Positional Parameters:
        -- An array of series names
        -- A function that returns True if the row starts a new group.

        """
        inp = self.stack.pop()
        result = inp.new(series_list)

        # create an index that given a column # in the output returns
        # a column # in the input
        idx = [ inp.series.index(seq) for seq in series_list ]

        if count:
            cnt = inp.new_set(DataSet.NUMERIC_DATA, inp.get_series_size(), [ count ])

        # seed the group_fn state so it knows when it's the 1st row
        self.group_data = None

        res_row = -1
        data_row = 0
        for i in range(0, inp.get_series_size()):
            if group_fn(self, inp, data_row):
                res_row += 1
                for out_col in range(0, len(idx)):
                    result[out_col, res_row] = inp[idx[out_col], data_row]
                if count:
                    cnt[0, res_row] = 1.0
                data_row += 1
            else:
                if count:
                    cnt[0, res_row] += 1.0
                data_row += 1
        if count:
            result.append(cnt)
        result.set_series_size(res_row)
        return self.stack.push(result)

    def diff(self, series_list, group_name=None, xfrm_suffix="_diff"):
        """Compute the difference of a series

        Pop the top of the stack and compute the difference, i.e.

            series['column-name'][m] - series['column-name'][m+1]

        for each column in the input. The result pushed to the stack
        will contain one fewer rows than the input.

        Positional Parameters:
        -- An array of series names

        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.diff,
                                 grp_len_fn=lambda src : len(src) - 1)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.diff,
                               xfrm_len_fn=lambda src : src.get_series_size() - 1)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

        series_names = [ ser + '_diff' for ser in series_list ]
        inp = self.stack.pop()
        res = inp.new_set(DataSet.NUMERIC_DATA, inp.get_series_size(), series_names)

        col = 0
        for ser in series_list:
            res[col] = np.diff(inp[ser][0:inp.get_series_size()])
            col += 1
        result = DataSet()
        result.append(res)
        result.set_series_size(inp.get_series_size() - 1)
        return self.stack.push(result)

    def _by_row(self, series_list, xfrm_suffix, xfrm_fn,
                xfrm_len_fn=lambda src : 1):
        series_names = [ ser + xfrm_suffix for ser in series_list ]
        inp = self.stack.pop()
        series_len = xfrm_len_fn(inp)
        res = inp.new_set(DataSet.NUMERIC_DATA, series_len, series_names)

        col = 0
        for ser in series_list:
            src = inp[ser][0:inp.get_series_size()]
            res[col] = xfrm_fn(src)
            col += 1
        res.set_series_size(series_len)
        return res

    def _by_group(self, series_list, group_name, xfrm_suffix, xfrm_fn,
                  xfrm_args=None, grp_len_fn=lambda src : 1):
        """Group data by series value

        Positional Parameters:
        -- An array of series names
        -- The series by which data will be grouped

        Keyword Parameters:
        xfrm_fn -- Perform a transform on each series
        """
        series_names = [ group_name ] + [ ser + xfrm_suffix for ser in series_list ]
        inp = self.stack.pop()

        # compute the unique values in the group_by series
        grp = inp[group_name]
        uniq = np.unique(grp)

        # compute the result size
        res_size = 0
        grp_start = {}
        grp_len = {}
        for value in uniq:
            grp_src = grp[grp == value]
            grp_len[value] = grp_len_fn(grp_src)
            grp_start[value] = res_size
            res_size += grp_len[value]

        # allocate the result
        res = inp.new_set(DataSet.NUMERIC_DATA, res_size, series_names)

        # copy the group data to the result
        start_row = 0
        for value in uniq:
            grp_dst = res[0]
            start_row = grp_start[value]
            grp_dst[start_row:start_row+grp_len[value]] = value

        col = 1
        for ser in series_list:
            src = inp[ser]
            for value in uniq:
                grp_src = src[grp == value]
                grp_dst = res[col]
                res_len = grp_len[value]
                start_row = grp_start[value]
                res_len = grp_len[value]
                if xfrm_args is None:
                    grp_dst[start_row:start_row+res_len] = xfrm_fn(grp_src)
                else:
                    grp_dst[start_row:start_row+res_len] = xfrm_fn(grp_src, xfrm_args)
            col += 1
        res.set_series_size(res_size)
        return res

    def _hist(self, a, args):
        print(a)
        print(args)

    def histogram(self, series_list, xfrm_suffix="_hist",
                  bins=10, range=None, weights=None, density=None):
        """Compute the histogram for each series

        Keyword Parameters:
        bins : int or sequence of scalars or str, optional

            If bins is an int, it defines the number of equal-width
            bins in the given range (10, by default). If bins is a
            sequence, it defines the bin edges, including the
            rightmost edge, allowing for non-uniform bin widths.

            If bins is a string, it defines the method used to
            calculate the optimal bin width, as defined by
            numpy.histogram_bin_edges.

        range : (float, float), optional

            The lower and upper range of the bins. If not provided,
            range is simply (series.min(), series.max()). Values
            outside the range are ignored. The first element of the
            range must be less than or equal to the second. range
            affects the automatic bin computation as well. While bin
            width is computed to be optimal based on the actual data
            within range, the bin count will fill the entire range
            including portions containing no data.

        weights : array_like, optional

            An array of weights, of the same shape as the series. Each
            value in the series only contributes its associated weight
            towards the bin count (instead of 1). If density is True,
            the weights are normalized, so that the integral of the
            density over the range remains 1.

        density : bool, optional

            If False, the result will contain the number of samples in
            each bin. If True, the result is the value of the
            probability density function at the bin, normalized such
            that the integral over the range is 1. Note that the sum
            of the histogram values will not be equal to 1 unless bins
            of unity width are chosen; it is not a probability mass
            function.

        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, self._hist)
        else:
            res = self._by_row(series_list, xfrm_suffix, self._hist)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def sum(self, series_list, group_name=None, xfrm_suffix="_sum"):
        """Compute sums for series across rows
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.sum)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.sum)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def mean(self, series_list, group_name=None, xfrm_suffix="_mean"):
        """Compute mean for series across rows
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.mean)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.mean)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def min(self, series_list, group_name=None, xfrm_suffix="_min"):
        """Compute min for series across rows
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.min)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.min)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def minrow(self, series):
        """Return the row with the minimum value in the series_list

        The result returned will have a single row containing the minimum value in the series specified.

        Positional Parameters:
        -- The name of the series
        """
        inp = self.stack.pop()
        res = inp.new_set(DataSet.NUMERIC_DATA, 1, inp.series)

        src = inp[series][0:inp.get_series_size()]
        row = np.argmin(src)
        for col in range(0, inp.get_series_count()):
            res[col][0] = inp[col][row]
        res.set_series_size(1)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def max(self, series_list, group_name=None, xfrm_suffix="_max"):
        """Compute min for series across rows
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.max)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.max)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def maxrow(self, series):
        """Return the row with the maximum value in the series

        The result returned will have a single row containing the max
        value of the series specified.

        Positional Parameters:
        -- The name of the series
        """
        inp = self.stack.pop()
        res = inp.new_set(DataSet.NUMERIC_DATA, 1, inp.series)

        src = inp[series][0:inp.get_series_size()]
        row = np.argmax(src)
        for col in range(0, inp.get_series_count()):
            res[col][0] = inp[col][row]
        res.set_series_size(1)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def std(self, series_list, group_name=None, xfrm_suffix="_std"):
        """Compute the standard deviation of a series

        See numpy.std for more information.

        Positional Parameters:
        -- An array of series names

        Keyword Parameters:
        group_by -- The name of a series by which data is grouped.
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.std)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.std)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def _per_row(self, series_list, xfrm_suffix, xfrm_fn):
        series_names = [ ser + xfrm_suffix for ser in series_list ]
        inp = self.stack.pop()
        res = inp.new_set(DataSet.NUMERIC_DATA, inp.get_series_size(), series_names)

        col = 0
        for ser in series_list:
            src = inp[ser][0:inp.get_series_size()]
            res[col] = xfrm_fn(src)
            col += 1
        res.set_series_size(inp.get_series_size())
        return res

    def gradient(self, series_list, group_name=None, xfrm_suffix="_grad"):
        """Compute the gradient of a series

        See numpy.gradient for more information.

        Positional Parameters:
        -- An array of series names
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix,
                                 np.gradient, grp_len_fn=lambda src : len(src))
        else:
            res = self._per_row(series_list, xfrm_suffix, np.gradient)
        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def unique(self, series_name):
        """Return the unique values of a series

        See numpy.unique for more information.

        Positional Parameters:
        -- A series name
        """
        inp = self.stack.pop()

        nda = inp[series_name][0:inp.get_series_size()]
        u = np.unique(nda)
        res = inp.new_set(DataSet.NUMERIC_DATA, len(u), [ series_name + "_unique" ])
        res[0] = u

        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def add(self, series_list, result=None):
        """Add a sequence of series

        The result contains a single series as output that contains
        the sum of the input series.

        Postional Parameters:
        -- A list of at least two series names.

        """
        if result is None:
            series_name = series_list[0]
            for series in series_list[1:]:
                series_name += '+' + series
        else:
            series_name = result

        inp = self.stack.pop()
        res = inp.new_set(inp.NUMERIC_DATA, inp.get_series_size(), [ series_name ])

        res[0] = inp[series_list[0]]
        for series in series_list[1:]:
            res[0] += inp[series]

        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def subtract(self, series_list, result=None):
        """Subtract a sequence of series

        The result contains a single series as output that contains
        the difference of the input series.

        Postional Parameters:
        -- A list of at least two series names.
        """
        if result is None:
            series_name = series_list[0]
            for series in series_list[1:]:
                series_name += '-' + series
        else:
            series_name = result

        inp = self.stack.pop()
        res = inp.new_set(inp.NUMERIC_DATA, inp.get_series_size(), [ series_name ])

        res[0] = inp[series_list[0]]
        for series in series_list[1:]:
            res[0] -= inp[series]

        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def multiply(self, series_list, result=None):
        """Multiple a sequence of series together

        The result contains a single series as output that contains
        the product of the input series.

        Postional Parameters:
        -- A list of at least two series names.
        """
        if result is None:
            series_name = series_list[0]
            for series in series_list[1:]:
                series_name += '*' + series
        else:
            series_name = result

        inp = self.stack.pop()
        res = inp.new_set(inp.NUMERIC_DATA, inp.get_series_size(), [ series_name ])

        res[0] = inp[series_list[0]]
        for series in series_list[1:]:
            res[0] *= inp[series]

        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def divide(self, series_list, result=None):
        """Divide a sequence of series

        The result contains a single series as output that contains
        the division of the input series.

        Postional Parameters:
        -- A list of at least two series names.
        """
        if result is None:
            series_name = series_list[0]
            for series in series_list[1:]:
                series_name += '/' + series
        else:
            series_name = result

        inp = self.stack.pop()
        res = inp.new_set(inp.NUMERIC_DATA, inp.get_series_size(), [ series_name ])

        res[0] = inp[series_list[0]]
        for series in series_list[1:]:
            res[0] /= inp[series]

        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def append(self, series=None, source=None):
        """Append DataSet series on the stack

        Append series from TOP-1 to TOP and push the result. If the
        source keyword is specified, the DataSet is source instead of
        TOP~1.

        Positional Arguments:
        -- An array of series names.

        Keyword Arguments:
        source -- Specifies the DataSet that contains the
                  series. Default is TOP~1.

        """
        top = self.stack.pop()
        if source is None:
            source = self.stack.pop()
        top.append(source, series=series)
        return self.stack.push(top)

    def extract(self, series_list, rename=None, source=None, rows=None):
        """Extract series from a DataSet

        The result contains a the series from the first argument
        optionally renamed as defined by the rename keyword parameter.

        The rename keyword must be None or the same length as the
        first parameter. A None in the list is ignored.

        Postional Parameters:
        -- A list of at series names

        Keyword Parameters:
        rename -- A list of names to rename each series to.
        source -- The source DataSet to use instead of TOP
        rows   -- Specifies a subset of rows from the source
        """
        if rename is None:
            series_names = series_list
        else:
            if len(series_list) != len(rename):
                raise ValueError("The rename list must be the same length "
                                 "as the series list")
            series_names = []
            for i in range(0, len(rename)):
                if rename[i] is None:
                    series_names.append(series_list[i])
                else:
                    series_names.append(rename[i])

        if source is None:
            source = self.stack.pop()

        if rows is None:
            series_size = source.get_series_size()
        else:
            series_size = rows[1] - rows[0]
        res = source.new_set(source.NUMERIC_DATA, series_size, series_names)

        for i in range(0, len(series_list)):
            if rows is None:
                res[i] = source[series_list[i]]
            else:
                res[i] = source[series_list[i]][rows[0]:rows[1]]

        result = DataSet()
        result.append(res)
        return self.stack.push(result)

    def concat(self, source=None):
        """Concatenate series data from the two DataSet(s)

        Concatenate the data from TOP and TOP~1. If the source keyword
        is specified, source is used instead of TOP~1.

        Keyword Parameters:
        source -- The set to use as the second DataSet instead of TOP~1
        """
        s = self.stack.pop()
        if source is None:
            source = self.stack.pop()
        res = s.concat(source)
        return self.stack.push(res)

    def push(self, res):
        return self.stack.push(res)

    def pick(self, n):
        return self.stack.pick(n)

    def dup(self):
        return self.stack.dup()

    def swap(self):
        return self.stack.swap()

    def show(self):
        return self.stack.show()

    def top(self):
        return self.stack.top()

    def pop(self):
        return self.stack.pop()

    def __getitem__(self, op):
        return self.ops[op]
