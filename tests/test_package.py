from __future__ import annotations

import importlib.metadata

import quickdag as m


def test_version():
    assert importlib.metadata.version("quickdag") == m.__version__
