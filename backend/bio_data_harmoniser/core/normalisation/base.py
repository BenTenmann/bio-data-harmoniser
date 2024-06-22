from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class Normaliser(Protocol):
    def normalise(self, series: pd.Series) -> pd.Series: ...
