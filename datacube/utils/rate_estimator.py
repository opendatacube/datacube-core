# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from timeit import default_timer as t_now
from types import SimpleNamespace

from typing_extensions import override


class RateEstimator:
    def __init__(self) -> None:
        self.t0 = t_now()
        self.t_last = self.t0
        self.n = 0

    def _compute(self) -> SimpleNamespace:
        dt = self.t_last - self.t0
        fps = 0 if self.n == 0 else self.n / dt
        return SimpleNamespace(elapsed=dt, n=self.n, fps=fps)

    def stats(self) -> SimpleNamespace:
        return self._compute()

    def every(self, k: int) -> bool:
        return (self.n % k) == 0

    def __call__(self, n: int = 1) -> None:
        self.t_last = t_now()
        self.n += n

    @override
    def __str__(self) -> str:
        state = self._compute()
        return f"N: {state.n:6,d} T: {state.elapsed:6.1f}s FPS: {state.fps:4.1f}"

    @override
    def __repr__(self) -> str:
        return f"<t0:{self.t0}, t_last:{self.t_last}, n:{self.n}>"
