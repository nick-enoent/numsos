import numpy as np
from numsos.Stack import Stack
from sosdb.DataSet import DataSet

class Transform(object):
    def __init__(self, dataSrc, dataSink, limit=1024, intervalMs=None):
        self.source = dataSrc
        self.sink = dataSink
        self.stack = Stack()
        self.window = limit
        self.interval_ms = intervalMs
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
        if self.interval_ms:
            result = self.source.get_results(limit=limit, keep=keep, reset=reset, wait=wait, interval_ms=intervalMs)
        else:
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
        if self.interval_ms:
            return self._next(count=count, wait=wait, keep=keep, reset=False, interval_ms=self.interval_ms)
        return self._next(count=count, wait=wait, keep=keep, reset=False)

    def diff(self, series_list, group_name=None, xfrm_suffix="_diff", keep=None, axis=-1):
        """Compute the difference of a series

        Pop the top of the stack and compute the difference, i.e.

            series['column-name'][m] - series['column-name'][m+1]

        for each column in the input. The result pushed to the stack
        will contain one fewer rows than the input.

        The 'group_name' parameter can be used to perform the
        difference on subsets of the series where other series in the
        same row have the same value.

        The 'xfrm_suffix' parameter specifies a string to append to
        the series names in the output.

        The 'keep' parameter specifies series in the input that are
        not specified in series_list but are to be retained in the
        output.  This is useful since diff reduces the size of the
        output series by the number of unique values in the group
        series.

        Positional Parameters:
        -- An array of series names

        Keyword Parameters:
        group_name  -- The name of a series to group data together
        xfrm_suffix -- A string to append to the series names to
                       note that they are the difference of the
                       original series.
        keep        -- An array of series names to retain in the
                       output in addition to the series_list.

        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.diff,
                                 xfrm_len_fn=lambda src : len(src) - 1,
                                 keep=keep, axis=axis)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.diff,
                               xfrm_len_fn=lambda src : src.get_series_size() - 1,
                               axis=axis)
        return self.stack.push(res)

    def _clone(self, inp, src_names, dst_names, res_size, xfrm_len_fn, axis):
        types = {}
        shapes = {}
        for col in range(0, len(dst_names)):
            ser = src_names[col]
            src = inp.array(ser)
            types[dst_names[col]] = src.dtype
            if src.ndim > 1:
                if axis == 1:
                    cols = xfrm_len_fn(src[0])
                    if cols > 1:
                        shapes[dst_names[col]] = ( src.shape[0], cols )
                    else:
                        shapes[dst_names[col]] = ( src.shape[0] )
                else:
                    shapes[dst_names[col]] = src.shape
            else:
                shapes[dst_names[col]] = src.shape

        return DataSet().new(res_size, dst_names, shapes=shapes, types=types)

    def _by_row(self, series_list, xfrm_suffix, xfrm_fn,
                xfrm_len_fn=lambda src : 1, **kwargs):
        series_names = [ ser + xfrm_suffix for ser in series_list ]
        inp = self.stack.pop()

        if 'axis' in kwargs:
            axis = kwargs['axis']
        else:
            axis = 0

        if axis == 0:
            series_len = xfrm_len_fn(inp)
        else:
            series_len = inp.get_series_size()

        res = self._clone(inp, series_list, series_names, series_len, xfrm_len_fn, axis)

        col = 0
        for ser in series_list:
            src = inp.array(ser)[0:inp.series_size]
            nda = res.array(col)
            nda[:] = xfrm_fn(src, **kwargs)
            col += 1
        return res

    def group(self, series_name, value):
        dataSet = DataSet()
        inp = self.pop()
        grp_ser = inp.array(series_name)
        grp_mask = grp_ser == value
        grp_len = len(grp_ser[grp_mask])
        for name in inp.series:
            ser = inp.array(name)
            grp = ser[grp_mask]
            dataSet.append_array(grp_len, name, grp)
        dataSet.set_series_size(grp_len)
        self.push(dataSet)
        return dataSet

    def _for_each(self, series_list, xfrm_fn, values):
        if len(series_list) == 0:
            return xfrm_fn(values)
        ser = series_list.pop(0)
        data = self.dup()
        self.unique(ser)
        u = self.pop()
        l = len(values)
        values.append(0)
        for v in u[0]:
            # All rows where top[ser] == v
            self.push(data)
            self.group(ser, v)
            values[l] = v
            self._for_each(series_list, xfrm_fn, values)

    def for_each(self, series_list, xfrm_fn):
        ser = series_list.pop(0)
        data = self.dup()
        self.unique(ser)
        unique_values = self.pop()[0]
        for v in unique_values:
            # All rows where top[ser] == v
            self.push(data)
            self.group(ser, v)
            self._for_each(series_list, xfrm_fn, [ v ])

    def _by_group(self, series_list, group_name, xfrm_suffix, xfrm_fn,
                  xfrm_len_fn=lambda src : 1, keep=None,
                  **kwargs):
        """Group data by a series value

        The transform function is performed over each group of data
        where the values in the group_name series are equal.

        The 'keep' list names the series that are to be copied
        (unmodified) to the result. If the series in the keep list are
        not of the same type as the series in the series_list or
        cannot be cast to that type, a new array of the appropriate
        type is allocated. This should be considered (for efficiency)
        when performing a sequence of computations.

        Positional Parameters:
        -- An array of series names
        -- The series by which data will be grouped

        Keyword Parameters:
        xfrm_fn -- The transform to perform on each series in the
                   series_list
        keep -- Series in the input that are not in the series_list
                that are to be retained in the output.
        **kwargs -- passed to the xfrm_fn function.
        """
        if keep is None:
            keep = []
        if group_name not in keep:
            keep.insert(0, group_name)
        dst_names = keep + [ ser + xfrm_suffix for ser in series_list ]
        src_names = keep + [ ser for ser in series_list ]
        inp = self.stack.pop()

        # compute the unique values in the group_by series
        grp = inp.array(group_name)
        uniq = np.unique(grp)

        # compute the result size
        res_size = 0
        grp_start = {}
        grp_len = {}
        if 'axis' in kwargs:
            axis = kwargs['axis']
        else:
            axis = 0            # over rows
        for value in uniq:
            grp_src = grp[grp == value]
            if axis == 0:
                grp_len[value] = xfrm_len_fn(grp_src)
            else:
                grp_len[value] = len(grp_src)
            grp_start[value] = res_size
            res_size += grp_len[value]

        # Allocate the result arrays.
        res = self._clone(inp, src_names, dst_names, res_size, xfrm_len_fn, axis)

        # copy the group and keep data to the result. src_names and
        # dst_names are the same for keep columns
        start_row = 0
        for value in uniq:
            for col in range(0, len(keep)):
                name = keep[col]
                src = inp.array(name)
                grp_src = src[grp == value]
                grp_dst = res.array(name)
                start_row = grp_start[value]
                res_len = grp_len[value]
                grp_dst[start_row:start_row+res_len] = grp_src[0:res_len]

        ser_col = len(keep)
        for col in range(ser_col, ser_col + len(series_list)):
            src = inp.array(src_names[col])
            for value in uniq:
                grp_src = src[grp == value]
                grp_dst = res.array(dst_names[col])
                start_row = grp_start[value]
                res_len = grp_len[value]
                grp_dst[start_row:start_row+res_len] = xfrm_fn(grp_src, **kwargs)
            col += 1
        return res

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

        Result:

        For each series in the input list, histogram() pushes two
        DataSets; one containing a series of bin values, and the other
        containing an array of bin edges. The bin-edges DataSet has
        one more datum than the bin values.
        """
        hist = DataSet()
        edges = DataSet()
        inp = self.stack.pop()
        series_size = inp.get_series_size()
        for ser in series_list:
            src = inp.array(ser)[0:series_size]
            res = np.histogram(src, bins=bins, range=range,
                               weights=weights, density=density)
            hist.append_array(len(res[0]), ser + xfrm_suffix, res[0])
            edges.append_array(len(res[1]), ser + "_edges", res[1])
        self.stack.push(hist)
        return self.stack.push(edges)

    def sum(self, series_list, group_name=None, xfrm_suffix="_sum", keep=None, **kwargs):
        """Compute sums for series across rows or columns
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.sum, keep=keep, **kwargs)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.sum, **kwargs)
        return self.stack.push(res)

    def mean(self, series_list, group_name=None, xfrm_suffix="_mean", keep=None, **kwargs):
        """Compute mean for series across rows
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.mean, keep=keep, **kwargs)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.mean, **kwargs)
        return self.stack.push(res)

    def min(self, series_list, group_name=None, xfrm_suffix="_min", keep=None, **kwargs):
        """Compute min for series across rows
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.min, keep=keep)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.min)
        return self.stack.push(res)

    def minrow(self, series):
        """Return the row with the minimum value in the series_list

        The result returned will have a single row containing the
        minimum value in the series specified.

        Positional Parameters:
        -- The name of the series

        """
        inp = self.stack.pop()
        res = DataSet().new(1, inp.series)

        src = inp.array(series)[0:inp.get_series_size()]
        row = np.argmin(src)
        for col in range(0, inp.get_series_count()):
            res[col,0] = inp.array(col)[row]
        return self.stack.push(res)

    def max(self, series_list, group_name=None, xfrm_suffix="_max", keep=None, **kwargs):
        """Compute max for series across rows
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.max, keep=keep, **kwargs)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.max, **kwargs)
        return self.stack.push(res)

    def maxrow(self, series):
        """Return the row with the maximum value in the series

        The result returned will have a single row containing the max
        value of the series specified.

        Positional Parameters:
        -- The name of the series
        """
        inp = self.stack.pop()
        res = DataSet().new(1, inp.series)

        src = inp.array(series)[0:inp.get_series_size()]
        row = np.argmax(src)
        for col in range(0, inp.get_series_count()):
            res[col,0] = inp.array(col)[row]
        return self.stack.push(res)

    def std(self, series_list, group_name=None, xfrm_suffix="_std", keep=None, **kwargs):
        """Compute the standard deviation of a series

        See numpy.std for more information.

        Positional Parameters:
        -- An array of series names

        Keyword Parameters:
        group_by -- The name of a series by which data is grouped.
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.std, keep=keep, **kwargs)
        else:
            res = self._by_row(series_list, xfrm_suffix, np.std, **kwargs)
        return self.stack.push(res)

    def _per_row(self, series_list, xfrm_suffix, xfrm_fn):
        series_names = [ ser + xfrm_suffix for ser in series_list ]
        inp = self.stack.pop()
        res = DataSet().new(inp.get_series_size(), series_names)

        col = 0
        for ser in series_list:
            src = inp.array(ser)[0:inp.series_size]
            res[col] = xfrm_fn(src)
            col += 1
        res.set_series_size(inp.get_series_size())
        return res

    def gradient(self, series_list, group_name=None, xfrm_suffix="_grad", keep=None, **kwargs):
        """Compute the gradient of a series

        See numpy.gradient for more information.

        Positional Parameters:
        -- An array of series names
        """
        if group_name:
            res = self._by_group(series_list, group_name, xfrm_suffix, np.gradient,
                                 grp_len_fn=lambda src : len(src),
                                 col_res_fn=lambda col : len(col),
                                 keep=keep, **kwargs)
        else:
            res = self._per_row(series_list, xfrm_suffix, np.gradient, **kwargs)
        return self.stack.push(res)

    def unique(self, series_name, result=None):
        """Return the unique values of a series

        See numpy.unique for more information.

        Positional Parameters:
        -- A series name
        """
        inp = self.stack.pop()
        nda = inp.array(series_name)[0:inp.get_series_size()]
        u = np.unique(nda)

        if result == None:
            result = series_name + "_unique"
        res = DataSet()
        res.append_array(len(u), result, u)
        return self.stack.push(res)

    def add(self, lhs, rhs, result=None):
        """Add a sequence of series

        The result contains a single series as output that contains
        the sum of the input series.

        Postional Parameters:
        -- A list of at least two series names.

        """
        inp = self.stack.pop()
        res = inp[lhs] + inp[rhs]
        if result:
            res.rename(0, result)
        return self.stack.push(res)

    def subtract(self, lhs, rhs, result=None):
        """Subtract a sequence of series

        The result contains a single series as output that contains
        the difference of the input series.

        Postional Parameters:
        -- A list of at least two series names.
        """
        inp = self.stack.pop()
        res = inp[lhs] - inp[rhs]
        if result:
            res.rename(0, result)
        return self.stack.push(res)

    def multiply(self, lhs, rhs, result=None):
        """Multiple a sequence of series together

        The result contains a single series as output that contains
        the product of the input series.

        Postional Parameters:
        -- A list of at least two series names.

        Keyword Parameters:
        result -- The name of the output series
        """
        inp = self.stack.pop()
        res = inp[lhs] * inp[rhs]
        if result:
            res.rename(0, result)
        return self.stack.push(res)

    def divide(self, lhs, rhs, result=None, nan=0.0):
        """Divide a sequence of series

        The result contains a single series as output that contains
        the division of the input series.

        Postional Parameters:
        -- A list of at least two series names.

        Keyword Parameters:
        nan -- Specifies a value to use for NaN results such
               as divide by zero.
        """
        inp = self.stack.pop()
        res = inp[lhs] / inp[rhs]
        if result:
            res.rename(0, result)
        return self.stack.push(res)

    def append_series(self, series_list=None, source=None):
        """Append series

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
        top.append_series(source, series_list=series_list)
        return self.stack.push(top)

    def extract(self, series_list, rename=None, source=None, rows=None):
        """Extract series from a DataSet

        The result contains the series from the first argument
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

        res = DataSet().new(series_size, series_names)

        for i in range(0, len(series_list)):
            if rows is None:
                res[i] = source[series_list[i]]
            else:
                res[i] = source[series_list[i]][rows[0]:rows[1]]

        return self.stack.push(res)

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

    def drop(self):
        return self.stack.drop()

    def __getitem__(self, op):
        return self.ops[op]
