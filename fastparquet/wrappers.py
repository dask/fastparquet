import numpy as np


class IndexedNullable:
    
    def __init__(self, index, data):
        self.index = index
        self.data = data
    
    def __getitem__(self, item):
        if isinstance(item, int):
            ind = self.index[item]
            return self.data[ind] if ind > 0 else None
        elif isinstance(item, (np.ndarray, slice)):
            item = np.atleast_1d(item)
            return IndexedNullable(self.index[item], self.data)
        else:
            raise TypeError

    def to_masked(self):
        data = np.empty(len(self.index), dtype=self.data.dtype)
        mask = self.index >= 0
        data[mask] = self.data
        return MaskedNullable(mask, data)

    def __len__(self):
        return len(self.index)


class MaskedNullable:
    
    def __init__(self, mask, data) -> None:
        self.mask = mask
        self.data = data
    
    def __getitem__(self, item):
        if isinstance(item, int):
            m = self.mask[item]
            return self.data[item] if m else None
        elif isinstance(item, (np.ndarray, slice)):
            return MaskedNullable(self.mask[item], self.data[item])
        else:
            raise TypeError

    def to_indexed(self):
        data = self.data[self.mask]
        index = np.empty(len(data), dtype="int64")
        index[self.mask == 0] = -1
        index[self.mask] = np.arange(len(data))  # could collect uniques
        return IndexedNullable(index, data)

    def __len__(self):
        return len(self.data)


class String:
    def __init__(self, offsets, data) -> None:
        # TODO: make this start/stop so we can take from length-data encoding?
        self.offsets = offsets
        self.data = data

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.data[self.offsets[item]: self.offsets[item + 1]].tobytes().decode()
        elif isinstance(item, slice):
            assert item.step is None
            if item.stop is None or item.stop == -1:
                stop = None
            else:
                stop = item.stop + 1
            return String(self.offsets[item.start:stop], self.data)
        elif isinstance(item, np.ndarray):
            # completely repacks the data
            # or make indexed/masked array? But what if they are then
            # indexed?
            raise NotImplementedError
        else:
            raise TypeError

    def __len__(self):
        return len(self.offsets) - 1


class Record:
    def __init__(self, fields: list=None, contents: list=None, data: dict=None):
        if data is None:
            data = {f: c for f, c in zip(fields, contents)}
        else:
            if fields is not None or data is not None:
                raise ValueError
        self.data = data

    def __getitem__(self, item):
        if isinstance(item, str):
            return self.data[item]
        elif isinstance(item, int):
            return {f: c[item] for f, c in self.data.items()}
        else:
            return Record(data={f: c[item] for f, c in self.data})

    def __len__(self):
        if self.data:
            return len(list(self.data.values())[0])
        return 0
