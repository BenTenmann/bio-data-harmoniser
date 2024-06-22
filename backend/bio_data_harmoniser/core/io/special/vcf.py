import io
import re

import pandas as pd

HEADER_PATTERN = re.compile(r"\s+")
SAMPLE_METADATA_PATTERN = re.compile(r"##SAMPLE=<(.+)>")
CONTIG_PATTERN = re.compile(r"##contig=<(.+)>")


def read_vcf(file: io.BytesIO) -> pd.DataFrame:
    contigs = {}
    sample_metadata = {}
    for line in file:
        if line.startswith(b"##contig"):
            match = CONTIG_PATTERN.match(line.decode("utf-8"))
            if match:
                contig = {
                    key: value
                    for key, value in (
                        field.split("=") for field in match.group(1).split(",")
                    )
                }
                contigs[contig["ID"]] = contig["assembly"]
            continue
        if line.startswith(b"##SAMPLE"):
            # each column after FORMAT is a sample
            # for now, we assume that there is only a single sample (as we have with OpenGWAS)
            match = SAMPLE_METADATA_PATTERN.match(line.decode("utf-8"))
            if match:
                sample_metadata.update(
                    {
                        key: value
                        for key, value in (
                            field.split("=") for field in match.group(1).split(",")
                        )
                    }
                )
            continue
        if line.startswith(b"#CHROM"):
            header = [
                col.strip()
                for col in HEADER_PATTERN.split(line.decode("utf-8"))
                if col.strip()
            ]
            break
    else:
        raise RuntimeError("`#CHROM` not found in VCF file")
    ix = header.index("FORMAT")
    for i in range(ix + 1, len(header)):
        header[i] = f"SAMPLE_{i - ix - 1}"
    df = pd.read_csv(file, names=header, sep="\t", dtype_backend="pyarrow")
    df["BUILD"] = df["#CHROM"].astype(str).map(contigs)
    return df.assign(**sample_metadata)
