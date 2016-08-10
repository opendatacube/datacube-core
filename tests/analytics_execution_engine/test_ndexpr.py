from __future__ import absolute_import
import os
import sys
import xarray as xr
import numpy as np
import pytest

from datacube.ndexpr import NDexpr

#
# Test cases for NDexpr class
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


def test_1():
    test_ndexpr = NDexpr()


def test_2():
    # perform language test

    ne = NDexpr()
    ne.set_ae(False)
    x1 = xr.DataArray(np.random.randn(2, 3))
    y1 = xr.DataArray(np.random.randn(2, 3))
    z1 = xr.DataArray(np.array([[[0,  1,  2], [3,  4,  5], [6,  7,  8]],
                                [[9, 10, 11], [12, 13, 14], [15, 16, 17]],
                                [[18, 19, 20], [21, 22, 23], [24, 25, 26]]
                                ]))
    z2 = z1*2
    z3 = np.arange(27)
    mask1 = z1 > 4

    assert ne.test("arccos(z1)", xr.ufuncs.arccos(z1))
    assert ne.test("angle(z1)", xr.ufuncs.angle(z1))
    assert ne.test("arccos(z1)", xr.ufuncs.arccos(z1))
    assert ne.test("arccosh(z1)", xr.ufuncs.arccosh(z1))
    assert ne.test("arcsin(z1)", xr.ufuncs.arcsin(z1))
    assert ne.test("arcsinh(z1)", xr.ufuncs.arcsinh(z1))
    assert ne.test("arctan(z1)", xr.ufuncs.arctan(z1))
    assert ne.test("arctanh(z1)", xr.ufuncs.arctanh(z1))
    assert ne.test("ceil(z1)", xr.ufuncs.ceil(z1))
    assert ne.test("conj(z1)", xr.ufuncs.conj(z1))
    assert ne.test("cos(z1)", xr.ufuncs.cos(z1))
    assert ne.test("cosh(z1)", xr.ufuncs.cosh(z1))
    assert ne.test("deg2rad(z1)", xr.ufuncs.deg2rad(z1))
    assert ne.test("degrees(z1)", xr.ufuncs.degrees(z1))
    assert ne.test("exp(z1)", xr.ufuncs.exp(z1))
    assert ne.test("expm1(z1)", xr.ufuncs.expm1(z1))
    assert ne.test("fabs(z1)", xr.ufuncs.fabs(z1))
    assert ne.test("fix(z1)", xr.ufuncs.fix(z1))
    assert ne.test("floor(z1)", xr.ufuncs.floor(z1))
    assert ne.test("frexp(z3)", xr.ufuncs.frexp(z3))
    assert ne.test("imag(z1)", xr.ufuncs.imag(z1))
    assert ne.test("iscomplex(z1)", xr.ufuncs.iscomplex(z1))
    assert ne.test("isfinite(z1)", xr.ufuncs.isfinite(z1))
    assert ne.test("isinf(z1)", xr.ufuncs.isinf(z1))
    assert ne.test("isnan(z1)", xr.ufuncs.isnan(z1))
    assert ne.test("isreal(z1)", xr.ufuncs.isreal(z1))
    assert ne.test("log(z1)", xr.ufuncs.log(z1))
    assert ne.test("log10(z1)", xr.ufuncs.log10(z1))
    assert ne.test("log1p(z1)", xr.ufuncs.log1p(z1))
    assert ne.test("log2(z1)", xr.ufuncs.log2(z1))
    assert ne.test("pow(z1, 2.0)", np.power(z1, 2.0))
    assert ne.test("pow(z1, 2)", np.power(z1, 2))
    assert ne.test("z1^2", np.power(z1, 2))
    assert ne.test("z1**2", np.power(z1, 2))
    assert ne.test("rad2deg(z1)", xr.ufuncs.rad2deg(z1))
    assert ne.test("radians(z1)", xr.ufuncs.radians(z1))
    assert ne.test("real(z1)", xr.ufuncs.real(z1))
    assert ne.test("rint(z1)", xr.ufuncs.rint(z1))
    assert ne.test("sign(z1)", xr.ufuncs.sign(z1))
    assert ne.test("signbit(z1)", xr.ufuncs.signbit(z1))
    assert ne.test("sin(z1)", xr.ufuncs.sin(z1))
    assert ne.test("sinh(z1)", xr.ufuncs.sinh(z1))
    assert ne.test("sqrt(z1)", xr.ufuncs.sqrt(z1))
    assert ne.test("square(z1)", xr.ufuncs.square(z1))
    assert ne.test("tan(z1)", xr.ufuncs.tan(z1))
    assert ne.test("tanh(z1)", xr.ufuncs.tanh(z1))
    assert ne.test("trunc(z1)", xr.ufuncs.trunc(z1))

    assert ne.test("arctan2(z1, z2)", xr.ufuncs.arctan2(z1, z2))
    assert ne.test("copysign(z1, z2)", xr.ufuncs.copysign(z1, z2))
    assert ne.test("fmax(z1, z2)", xr.ufuncs.fmax(z1, z2))
    assert ne.test("fmin(z1, z2)", xr.ufuncs.fmin(z1, z2))
    assert ne.test("fmod(z1, z2)", xr.ufuncs.fmod(z1, z2))
    assert ne.test("hypot(z1, z2)", xr.ufuncs.hypot(z1, z2))
    assert ne.test("ldexp(z1, z2)", xr.DataArray(xr.ufuncs.ldexp(z1, z2)))
    assert ne.test("logaddexp(z1, z2)", xr.ufuncs.logaddexp(z1, z2))
    assert ne.test("logaddexp2(z1, z2)", xr.ufuncs.logaddexp2(z1, z2))
    assert ne.test("logicaland(z1, z2)", xr.ufuncs.logical_and(z1, z2))
    assert ne.test("logicalnot(z1, z2)", xr.ufuncs.logical_not(z1, z2))
    assert ne.test("logicalor(z1, z2)", xr.ufuncs.logical_or(z1, z2))
    assert ne.test("logicalxor(z1, z2)", xr.ufuncs.logical_xor(z1, z2))
    assert ne.test("maximum(z1, z2)", xr.ufuncs.maximum(z1, z2))
    assert ne.test("minimum(z1, z2)", xr.ufuncs.minimum(z1, z2))
    assert ne.test("nextafter(z1, z2)", xr.ufuncs.nextafter(z1, z2))

    assert ne.test("all(z1)", xr.DataArray.all(z1))
    assert ne.test("all(z1, 0)", xr.DataArray.all(z1, axis=0))
    assert ne.test("all(z1, 0, 1)", xr.DataArray.all(z1, axis=(0, 1)))
    assert ne.test("all(z1, 0, 1, 2)", xr.DataArray.all(z1, axis=(0, 1, 2)))

    assert ne.test("any(z1)", xr.DataArray.any(z1))
    assert ne.test("any(z1, 0)", xr.DataArray.any(z1, axis=0))
    assert ne.test("any(z1, 0, 1)", xr.DataArray.any(z1, axis=(0, 1)))
    assert ne.test("any(z1, 0, 1, 2)", xr.DataArray.any(z1, axis=(0, 1, 2)))

    assert ne.test("argmax(z1)", xr.DataArray.argmax(z1))
    assert ne.test("argmax(z1, 0)", xr.DataArray.argmax(z1, axis=0))
    assert ne.test("argmax(z1, 1)", xr.DataArray.argmax(z1, axis=1))
    assert ne.test("argmax(z1, 2)", xr.DataArray.argmax(z1, axis=2))

    assert ne.test("argmin(z1)", xr.DataArray.argmin(z1))
    assert ne.test("argmin(z1, 0)", xr.DataArray.argmin(z1, axis=0))
    assert ne.test("argmin(z1, 1)", xr.DataArray.argmin(z1, axis=1))
    assert ne.test("argmin(z1, 2)", xr.DataArray.argmin(z1, axis=2))

    assert ne.test("max(z1)", xr.DataArray.max(z1))
    assert ne.test("max(z1, 0)", xr.DataArray.max(z1, axis=0))
    assert ne.test("max(z1, 0, 1)", xr.DataArray.max(z1, axis=(0, 1)))
    assert ne.test("max(z1, 0, 1, 2)", xr.DataArray.max(z1, axis=(0, 1, 2)))

    assert ne.test("mean(z1)", xr.DataArray.mean(z1))
    assert ne.test("mean(z1, 0)", xr.DataArray.mean(z1, axis=0))
    assert ne.test("mean(z1, 0, 1)", xr.DataArray.mean(z1, axis=(0, 1)))
    assert ne.test("mean(z1, 0, 1, 2)", xr.DataArray.mean(z1, axis=(0, 1, 2)))

    assert ne.test("median(z1)", xr.DataArray.median(z1))
    assert ne.test("median(z1, 0)", xr.DataArray.median(z1, axis=0))
    assert ne.test("median(z1, 0, 1)", xr.DataArray.median(z1, axis=(0, 1)))
    assert ne.test("median(z1, 0, 1, 2)", xr.DataArray.median(z1, axis=(0, 1, 2)))

    assert ne.test("min(z1)", xr.DataArray.min(z1))
    assert ne.test("min(z1, 0)", xr.DataArray.min(z1, axis=0))
    assert ne.test("min(z1, 0, 1)", xr.DataArray.min(z1, axis=(0, 1)))
    assert ne.test("min(z1, 0, 1, 2)", xr.DataArray.min(z1, axis=(0, 1, 2)))

    assert ne.test("prod(z1)", xr.DataArray.prod(z1))
    assert ne.test("prod(z1, 0)", xr.DataArray.prod(z1, axis=0))
    assert ne.test("prod(z1, 0, 1)", xr.DataArray.prod(z1, axis=(0, 1)))
    assert ne.test("prod(z1, 0, 1, 2)", xr.DataArray.prod(z1, axis=(0, 1, 2)))

    assert ne.test("sum(z1)", xr.DataArray.sum(z1))
    assert ne.test("sum(z1, 0)", xr.DataArray.sum(z1, axis=0))
    assert ne.test("sum(z1, 0, 1)", xr.DataArray.sum(z1, axis=(0, 1)))
    assert ne.test("sum(z1, 0, 1, 2)", xr.DataArray.sum(z1, axis=(0, 1, 2)))

    assert ne.test("std(z1)", xr.DataArray.std(z1))
    assert ne.test("std(z1, 0)", xr.DataArray.std(z1, axis=0))
    assert ne.test("std(z1, 0, 1)", xr.DataArray.std(z1, axis=(0, 1)))
    assert ne.test("std(z1, 0, 1, 2)", xr.DataArray.std(z1, axis=(0, 1, 2)))

    assert ne.test("var(z1)", xr.DataArray.var(z1))
    assert ne.test("var(z1, 0)", xr.DataArray.var(z1, axis=0))
    assert ne.test("var(z1, 0, 1)", xr.DataArray.var(z1, axis=(0, 1)))
    assert ne.test("var(z1, 0, 1, 2)", xr.DataArray.var(z1, axis=(0, 1, 2)))

    assert ne.test("percentile(z1, 50)", np.percentile(z1, 50))
    assert ne.test("percentile(z1, 50)+percentile(z1, 50)", np.percentile(z1, 50) + np.percentile(z1, 50))
    assert ne.test("percentile(z1, (50))", np.percentile(z1, (50)))
    assert ne.test("percentile(z1, (50, 60))", np.percentile(z1, (50, 60)))
    assert ne.test("percentile(z1, (50, 60, 70))", np.percentile(z1, (50, 60, 70)))
    assert ne.test("percentile(z1, (50, 60, 70)) + percentile(z1, (50, 60, 70))",
                   np.percentile(z1, (50, 60, 70)) + np.percentile(z1, (50, 60, 70)))
    assert ne.test("1 + var(z1, 0, 0+1, 2) + 1", 1+xr.DataArray.var(z1, axis=(0, 0+1, 2))+1)

    assert ne.test("1 + ((z1+z1)*0.0005)**2 + 1", 1 + np.power((z1+z1)*0.0005, 2) + 1)

    assert ne.test("z1{mask1}", xr.DataArray.where(z1, mask1))
    assert ne.test("z1{z1>2}", xr.DataArray.where(z1, z1 > 2))
    assert ne.test("z1{z1>=2}", xr.DataArray.where(z1, z1 >= 2))
    assert ne.test("z1{z1<2}", xr.DataArray.where(z1, z1 < 2))
    assert ne.test("z1{z1<=2}", xr.DataArray.where(z1, z1 <= 2))
    assert ne.test("z1{z1==2}", xr.DataArray.where(z1, z1 == 2))
    assert ne.test("z1{z1!=2}", xr.DataArray.where(z1, z1 != 2))

    assert ne.test("z1{z1<2 | z1>5}", xr.DataArray.where(z1, (z1 < 2) | (z1 > 5)))
    assert ne.test("z1{z1>2 & z1<5}", xr.DataArray.where(z1, (z1 > 2) & (z1 < 5)))

    ne.evaluate("m = z1+1")
    assert ne.test("m", z1+1)

    assert ne.test("z1{~mask1}", xr.DataArray.where(z1, ~mask1))

    assert ne.test("(1<0?1+1;2+2)", 4)
    assert ne.test("(0<1?1+1;2+2)", 2)
    assert ne.test("z1+mask1", xr.DataArray.where(z1, mask1))

    assert ne.is_number(1) is True

    assert ne.test("-z1", -z1)
    assert ne.test("z1{!(z1<2)}", xr.DataArray.where(z1, xr.ufuncs.logical_not(z1 < 2)))

    assert ne.test("z1>>1", np.right_shift(z1, 1))
    assert ne.test("z1<<1", np.left_shift(z1, 1))


def test_3():
    # perform assignment stack level testing

    ne = NDexpr()

    ne.test_1_level()
    ne.test_2_level()
