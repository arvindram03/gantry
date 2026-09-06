# SPDX-License-Identifier: Apache-2.0
#
# The image a verification job runs in.
#
# Separate from the Beam image, and much smaller, because verifying an Iceberg
# table needs no Beam at all — it needs to read Parquet and compute one number.
#
# It exists so that Gantry does not read the data itself. A PostgreSQL target
# computes its own checksum because it has an engine to compute it in; an
# Iceberg table is a pile of Parquet files with nothing to send SQL to, and the
# alternative to a job here is streaming every row into the Gantry process,
# which is the one thing the design forbids. What comes back is one checksum and
# one row count, bounded by construction rather than by the size of the data.
#
# Gantry itself is installed so the checksum is *the same code* as the one the
# rest of the system uses. A second implementation of the normalisation living
# in a generated script would be a third place for the definition to drift.

FROM python:3.12-slim

RUN pip install --no-cache-dir "pyiceberg[pyarrow]==0.10.0"

# The wheel is built from the working tree by `make verify-image`, so the image
# and the code that reads its output cannot disagree about the checksum.
COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl

ENTRYPOINT []
