from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator


# Delta Maven coordinates per Spark minor version. Delta renamed its artifact at
# 4.1.0 to embed the Spark minor (``delta-spark_<spark-minor>_<scala>``); earlier
# releases (3.3.x, 4.0.x) keep the legacy ``delta-spark_<scala>`` name. A derived
# ``delta-spark_{minor}_{scala}`` rule for every Spark 4.x produced a 404 for 4.0
# (``delta-spark_4.0_2.13`` does not exist at 4.0.1), so pin the full coordinate
# explicitly per Spark version: a future Delta rename then cannot silently break
# the JVM classpath. Each row was verified against Maven Central:
#   Spark 3.5 -> io.delta:delta-spark_2.12:3.3.2
#   Spark 4.0 -> io.delta:delta-spark_2.13:4.0.1   (legacy name; 4.0 minor not embedded)
#   Spark 4.1 -> io.delta:delta-spark_4.1_2.13:4.1.0 (new name introduced at Delta 4.1.0)
DELTA_SPARK_COORDINATES: dict[str, str] = {
    "3.5": "io.delta:delta-spark_2.12:3.3.2",
    "4.0": "io.delta:delta-spark_2.13:4.0.1",
    "4.1": "io.delta:delta-spark_4.1_2.13:4.1.0",
}


def delta_spark_maven_coordinate(spark_version: str) -> str:
    """Resolve the delta-spark Maven coordinate for a Spark version.

    Raises ``RuntimeError`` for a Spark version with no mapping so callers can skip
    the JVM Delta tests rather than fail on an unresolvable classpath.
    """
    spark_minor = ".".join(spark_version.split(".")[:2])
    try:
        return DELTA_SPARK_COORDINATES[spark_minor]
    except KeyError as error:
        message = (
            f"No Delta Maven coordinate mapping for Spark {spark_version}; "
            "add an entry to DELTA_SPARK_COORDINATES in "
            "python/pysail/testing/spark/utils/jvm.py."
        )
        raise RuntimeError(message) from error


@contextlib.contextmanager
def classic_spark_mode() -> Generator[None, None, None]:
    old_api_mode = os.environ.get("SPARK_API_MODE")
    old_remote = os.environ.pop("SPARK_REMOTE", None)
    old_connect_mode = os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
    os.environ["SPARK_API_MODE"] = "classic"
    try:
        yield
    finally:
        if old_api_mode is None:
            os.environ.pop("SPARK_API_MODE", None)
        else:
            os.environ["SPARK_API_MODE"] = old_api_mode
        if old_remote is not None:
            os.environ["SPARK_REMOTE"] = old_remote
        if old_connect_mode is not None:
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = old_connect_mode
