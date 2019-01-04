import pandas as pd
from collections import Hashable

class DataFrameBuilder():
    '''
    Helper class for efficiently building Pandas dataframe with multi-level column indices
    '''

    def __init__(self, append_value=None):
        self.data = {}
        self.index = []
        self.columns = set()
        self.append_value = append_value

    def add_value(self, column, value):
        '''Add a value to the given column for the current index'''
        assert isinstance(column, Hashable)
        # assert isinstance(value, (float, int, str, None)) # Note that adding non-number objects will change the dataframe data type
        assert len(self.index) > 0

        if column not in self.columns:
            self.columns.add(column)
            self.data[column] = [self.append_value] * (len(self.index) - 1)
        
        if len(self.data[column]) == len(self.index) - 1:
            self.data[column].append(value)
        else:
            raise IndexError(f'Length {len(self.data[column])} of column for {column} not correct, should be {len(self.index)-1}')

    def new_row(self, index_value):
        '''Add a new row with the given index value'''
        self._finalize_row()
        self.index.append(index_value)

    def _finalize_row(self):
        '''Update the current row by appending each column that did not receive a value with self.append_value'''
        index_length = len(self.index)
        for c in self.columns:
            if len(self.data[c]) == index_length:
                continue
            elif len(self.data[c]) == index_length - 1:
                self.data[c].append(self.append_value)
            else:
                raise IndexError(f'Incorrect column length {len(self.data[c])} compared to {index_length} for column {c}')
        
        for c in self.columns:
            assert len(self.data[c]) == len(self.index)

    def to_df(self):
        '''Export to a pandas DataFrame'''
        self._finalize_row()
        return pd.DataFrame(self.data, index=self.index)

