import re
from collections import OrderedDict
from distutils.version import LooseVersion
import numpy as np
from pandas import (
    Categorical, DataFrame, Series,
    CategoricalIndex, RangeIndex, Index, MultiIndex,
    __version__ as pdver, DatetimeIndex
)
from pandas.api.types import is_categorical_dtype
import warnings


class Dummy(object):
    pass


def empty(types, size, cats=None, cols=None, index_types=None, index_names=None,
          timezones=None):
    """
    Create empty DataFrame to assign into

    In the simplest case, will return a Pandas dataframe of the given size,
    with columns of the given names and types. The second return value `views`
    is a dictionary of numpy arrays into which you can assign values that
    show up in the dataframe.

    For categorical columns, you get two views to assign into: if the
    column name is "col", you get both "col" (the category codes) and
    "col-catdef" (the category labels).

    For a single categorical index, you should use the `.set_categories`
    method of the appropriate "-catdef" columns, passing an Index of values

    ``views['index-catdef'].set_categories(pd.Index(newvalues), fastpath=True)``

    Multi-indexes work a lot like categoricals, even if the types of each
    index are not themselves categories, and will also have "-catdef" entries
    in the views. However, these will be Dummy instances, providing only a
    ``.set_categories`` method, to be used as above.

    Parameters
    ----------
    types: like np record structure, 'i4,u2,f4,f2,f4,M8,m8', or using tuples
        applies to non-categorical columns. If there are only categorical
        columns, an empty string of None will do.
    size: int
        Number of rows to allocate
    cats: dict {col: labels}
        Location and labels for categorical columns, e.g., {1: ['mary', 'mo]}
        will create column index 1 (inserted amongst the numerical columns)
        with two possible values. If labels is an integers, `{'col': 5}`,
        will generate temporary labels using range. If None, or column name
        is missing, will assume 16-bit integers (a reasonable default).
    cols: list of labels
        assigned column names, including categorical ones.
    index_types: list of str
        For one of more index columns, make them have this type. See general
        description, above, for caveats about multi-indexing. If None, the
        index will be the default RangeIndex.
    index_names: list of str
        Names of the index column(s), if using
    timezones: dict {col: timezone_str}
        for timestamp type columns, apply this timezone to the pandas series;
        the numpy view will be UTC.

    Returns
    -------
    - dataframe with correct shape and data-types
    - list of numpy views, in order, of the columns of the dataframe. Assign
        to this.
    """
    views = {}
    timezones = timezones or {}

    if isinstance(types, str):
        types = types.split(',')
    cols = cols if cols is not None else range(len(types))

    def cat(col):
        if cats is None or col not in cats:
            return RangeIndex(0, 2**14)
        elif isinstance(cats[col], int):
            return RangeIndex(0, cats[col])
        else:  # explicit labels list
            return cats[col]

    df = OrderedDict()
    for t, col in zip(types, cols):
        # Create empty arrays of length 1, so we can call `repeat` below
        if str(t) == 'category':
            df[str(col)] = Categorical([-1], categories=cat(col),
                                                 fastpath=True)
        else:
            if hasattr(t, 'base'):
                # funky pandas not-dtype
                t = t.base
            d = np.empty(1, dtype=t)
            if d.dtype.kind == "M" and str(col) in timezones:
                try:
                    d = Series(d).dt.tz_localize(timezones[str(col)])
                except:
                    warnings.warn("Inferring time-zone from %s in column %s "
                                  "failed, using time-zone-agnostic"
                                  "" % (timezones[str(col)], col))
            df[str(col)] = d

    df = DataFrame(df)
    if not index_types:
        index = RangeIndex(size)
    elif len(index_types) == 1:
        t, col = index_types[0], index_names[0]
        if col is None:
            raise ValueError('If using an index, must give an index name')
        if str(t) == 'category':
            c = Categorical([], categories=cat(col), fastpath=True)
            vals = np.zeros(size, dtype=c.codes.dtype)
            index = CategoricalIndex(c)
            index._data._codes = vals
            views[col] = vals
            views[col+'-catdef'] = index._data
        else:
            if hasattr(t, 'base'):
                # funky pandas not-dtype
                t = t.base
            d = np.empty(size, dtype=t)
            if d.dtype.kind == "M" and str(col) in timezones:
                # 1) create the DatetimeIndex in UTC as no datetime conversion is needed and
                # it works with d uninitialised data (no NonExistentTimeError or AmbiguousTimeError)
                # 2) convert to timezone (if UTC=noop, if None=remove tz, if other=change tz)
                index = DatetimeIndex(d, tz="UTC").tz_convert(timezones[str(col)])
            else:
                index = Index(d)
            views[col] = index.values
    else:
        index = MultiIndex([[]], [[]])
        # index = MultiIndex.from_arrays(indexes)
        index._levels = list()
        index._labels = list()
        index._codes = list()
        index._names = list(index_names)
        for i, col in enumerate(index_names):
            index._levels.append(Index([None]))

            def set_cats(values, i=i, col=col, **kwargs):
                values.name = col
                if index._levels[i][0] is None:
                    index._levels[i] = values
                elif not index._levels[i].equals(values):
                    raise RuntimeError("Different dictionaries encountered"
                                       " while building categorical")

            x = Dummy()
            x._set_categories = set_cats

            d = np.zeros(size, dtype=int)
            if LooseVersion(pdver) >= LooseVersion("0.24.0"):
                index._codes = list(index._codes) + [d]
            else:
                index._labels.append(d)
            views[col] = d
            views[col+'-catdef'] = x

    # Create DataFrame with same dtypes and desired length.
    df = DataFrame(
        {col: df[col]._values.repeat(size) for col in df.columns},
        index=index,
        columns=df.columns,
    )

    # create views
    for col in df.columns:
        vals = df[col]._values
        if isinstance(vals, np.ndarray):
            views[col] = vals
        elif is_categorical_dtype(vals):
            views[col] = vals._codes
            views[col+'-catdef'] = vals

        elif hasattr(vals.dtype, "tz"):
            # datetime64tz, get the ndarray directly backing it
            views[col] = vals._data

        else:
            # catchall, anything that gets here will be an ExtensionArray 
            views[col] = vals

    if index_names:
        df.index.names = [
            None if re.match(r'__index_level_\d+__', n) else n
            for n in index_names
        ]
    return df, views
