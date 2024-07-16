"""Module containing functionality for IO of different file-formats"""

import functools
import io
import gzip
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

import pandas as pd

from bio_data_harmoniser.core.io.special import vcf


def _write_no_op(_: pd.DataFrame, __: str) -> None:
    pass


@dataclass(frozen=True)
class Format:
    suffixes: tuple[str, ...]
    read: Callable[[io.BytesIO], pd.DataFrame]
    write: Callable[[pd.DataFrame, str], Any] = field(
        default_factory=lambda: _write_no_op
    )
    name: str = ""
    description: str = ""
    reference: str | None = None


def as_pandas_reader(
    fn: Callable[[io.BytesIO], pd.DataFrame], **kwargs
) -> Callable[[io.BytesIO], pd.DataFrame]:
    if "dtype_backend" not in kwargs:
        kwargs["dtype_backend"] = "pyarrow"
    return functools.partial(fn, **kwargs)


class SupportedFormats:
    csv = Format(
        name="csv",
        description="A file containing comma-separated values (CSV) data.",
        reference="https://en.wikipedia.org/wiki/Comma-separated_values",
        suffixes=(".csv",),
        read=as_pandas_reader(pd.read_csv),
        write=pd.DataFrame.to_csv,
    )
    tsv = Format(
        name="tsv",
        description="A file containing comma-separated values (TSV) data.",
        reference="https://en.wikipedia.org/wiki/Tab-separated_values",
        suffixes=(".tsv",),
        read=as_pandas_reader(pd.read_csv, sep="\t"),
        write=functools.partial(pd.DataFrame.to_csv, sep="\t"),
    )
    wsv = Format(
        name="wsv",
        description="A file containing comma-separated values (WSV) data.",
        suffixes=(".txt",),
        read=as_pandas_reader(pd.read_csv, sep=r"\s+"),
        write=functools.partial(pd.DataFrame.to_csv, sep=" "),
    )
    parquet = Format(
        name="parquet",
        description="A file in the Apache Parquet format.",
        reference="https://parquet.apache.org",
        suffixes=(".parquet", ".pq"),
        read=as_pandas_reader(pd.read_parquet),
        write=pd.DataFrame.to_parquet,
    )
    vcf = Format(
        name="vcf",
        description="A file in the Variant Call Format (VCF) format.",
        reference="https://samtools.github.io/hts-specs/VCFv4.3.pdf",
        suffixes=(".vcf",),
        read=vcf.read_vcf
    )
    excel = Format(
        name="excel",
        description="A file in the Excel format.",
        reference="https://en.wikipedia.org/wiki/Microsoft_Excel",
        suffixes=(".xls", ".xlsx"),
        read=as_pandas_reader(pd.read_excel),
        write=pd.DataFrame.to_excel,
    )

    @classmethod
    def get_formats(cls) -> list[Format]:
        return [fmt for fmt in vars(cls).values() if isinstance(fmt, Format)]

    @classmethod
    def as_suffix_lookup(cls) -> dict[str, Format]:
        return {suffix: fmt for fmt in cls.get_formats() for suffix in fmt.suffixes}


def get_format_from_suffix(
    filename: str, on_unsupported: Literal["error", "ignore"] = "error"
) -> Format | None:
    suffix = Path(filename).suffix
    if suffix.endswith(".gz"):
        # if the file is gzipped, we need to remove the .gz suffix to get the actual file suffix
        suffix = Path(filename).with_suffix("").suffix
    fmt = SupportedFormats.as_suffix_lookup().get(suffix)
    if fmt is None:
        if on_unsupported == "ignore":
            return
        raise NotImplementedError(
            f"File with `{suffix}` suffix is not recognised as a supported file format."
        )
    return fmt


def detect_filetype(file: io.BytesIO) -> Format:
    # TODO: we need to recognise the filetype not only from the suffix (which is a good proxy), but also from the
    #  actual file contents
    # for this, there a few options:
    # 1. python-magic
    # 2. Google ML model for file-type detection
    #
    # these options won't work very well for bio-specific filetypes; for those we may need to stick with suffixes for
    # now (or we can try using an LLM for that)
    return get_format_from_suffix(file.name)


def read(file: str | io.BytesIO) -> pd.DataFrame:
    is_str = isinstance(file, str)
    if is_str:
        if file.endswith(".gz"):
            file = gzip.open(file, "rb")
        else:
            file = open(file, "rb")
    filetype = detect_filetype(file)
    df = filetype.read(file)
    if is_str:
        file.close()
    return df
