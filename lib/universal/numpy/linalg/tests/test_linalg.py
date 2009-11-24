""" Test functions for linalg module
"""

import numpy as np
from numpy.testing import *
from numpy import array, single, double, csingle, cdouble, dot, identity
from numpy import multiply, atleast_2d, inf, asarray, matrix
from numpy import linalg
from numpy.linalg import matrix_power, norm

def ifthen(a, b):
    return not a or b

old_assert_almost_equal = assert_almost_equal
def imply(a, b):
    return not a or b

def assert_almost_equal(a, b, **kw):
    if asarray(a).dtype.type in (single, csingle):
        decimal = 6
    else:
        decimal = 12
    old_assert_almost_equal(a, b, decimal=decimal, **kw)

class LinalgTestCase:
    def test_single(self):
        a = array([[1.,2.], [3.,4.]], dtype=single)
        b = array([2., 1.], dtype=single)
        self.do(a, b)

    def test_double(self):
        a = array([[1.,2.], [3.,4.]], dtype=double)
        b = array([2., 1.], dtype=double)
        self.do(a, b)

    def test_csingle(self):
        a = array([[1.+2j,2+3j], [3+4j,4+5j]], dtype=csingle)
        b = array([2.+1j, 1.+2j], dtype=csingle)
        self.do(a, b)

    def test_cdouble(self):
        a = array([[1.+2j,2+3j], [3+4j,4+5j]], dtype=cdouble)
        b = array([2.+1j, 1.+2j], dtype=cdouble)
        self.do(a, b)

    def test_empty(self):
        a = atleast_2d(array([], dtype = double))
        b = atleast_2d(array([], dtype = double))
        try:
            self.do(a, b)
            raise AssertionError("%s should fail with empty matrices", self.__name__[5:])
        except linalg.LinAlgError, e:
            pass

    def test_nonarray(self):
        a = [[1,2], [3,4]]
        b = [2, 1]
        self.do(a,b)

    def test_matrix_b_only(self):
        """Check that matrix type is preserved."""
        a = array([[1.,2.], [3.,4.]])
        b = matrix([2., 1.]).T
        self.do(a, b)

    def test_matrix_a_and_b(self):
        """Check that matrix type is preserved."""
        a = matrix([[1.,2.], [3.,4.]])
        b = matrix([2., 1.]).T
        self.do(a, b)


class TestSolve(LinalgTestCase, TestCase):
    def do(self, a, b):
        x = linalg.solve(a, b)
        assert_almost_equal(b, dot(a, x))
        assert imply(isinstance(b, matrix), isinstance(x, matrix))

class TestInv(LinalgTestCase, TestCase):
    def do(self, a, b):
        a_inv = linalg.inv(a)
        assert_almost_equal(dot(a, a_inv), identity(asarray(a).shape[0]))
        assert imply(isinstance(a, matrix), isinstance(a_inv, matrix))

class TestEigvals(LinalgTestCase, TestCase):
    def do(self, a, b):
        ev = linalg.eigvals(a)
        evalues, evectors = linalg.eig(a)
        assert_almost_equal(ev, evalues)

class TestEig(LinalgTestCase, TestCase):
    def do(self, a, b):
        evalues, evectors = linalg.eig(a)
        assert_almost_equal(dot(a, evectors), multiply(evectors, evalues))
        assert imply(isinstance(a, matrix), isinstance(evectors, matrix))

class TestSVD(LinalgTestCase, TestCase):
    def do(self, a, b):
        u, s, vt = linalg.svd(a, 0)
        assert_almost_equal(a, dot(multiply(u, s), vt))
        assert imply(isinstance(a, matrix), isinstance(u, matrix))
        assert imply(isinstance(a, matrix), isinstance(vt, matrix))

class TestCondSVD(LinalgTestCase, TestCase):
    def do(self, a, b):
        c = asarray(a) # a might be a matrix
        s = linalg.svd(c, compute_uv=False)
        old_assert_almost_equal(s[0]/s[-1], linalg.cond(a), decimal=5)

class TestCond2(LinalgTestCase, TestCase):
    def do(self, a, b):
        c = asarray(a) # a might be a matrix
        s = linalg.svd(c, compute_uv=False)
        old_assert_almost_equal(s[0]/s[-1], linalg.cond(a,2), decimal=5)

class TestCondInf(TestCase):
    def test(self):
        A = array([[1.,0,0],[0,-2.,0],[0,0,3.]])
        assert_almost_equal(linalg.cond(A,inf),3.)

class TestPinv(LinalgTestCase, TestCase):
    def do(self, a, b):
        a_ginv = linalg.pinv(a)
        assert_almost_equal(dot(a, a_ginv), identity(asarray(a).shape[0]))
        assert imply(isinstance(a, matrix), isinstance(a_ginv, matrix))

class TestDet(LinalgTestCase, TestCase):
    def do(self, a, b):
        d = linalg.det(a)
        if asarray(a).dtype.type in (single, double):
            ad = asarray(a).astype(double)
        else:
            ad = asarray(a).astype(cdouble)
        ev = linalg.eigvals(ad)
        assert_almost_equal(d, multiply.reduce(ev))

class TestLstsq(LinalgTestCase, TestCase):
    def do(self, a, b):
        u, s, vt = linalg.svd(a, 0)
        x, residuals, rank, sv = linalg.lstsq(a, b)
        assert_almost_equal(b, dot(a, x))
        assert_equal(rank, asarray(a).shape[0])
        assert_almost_equal(sv, sv.__array_wrap__(s))
        assert imply(isinstance(b, matrix), isinstance(x, matrix))
        assert imply(isinstance(b, matrix), isinstance(residuals, matrix))

class TestMatrixPower(TestCase):
    R90 = array([[0,1],[-1,0]])
    Arb22 = array([[4,-7],[-2,10]])
    noninv = array([[1,0],[0,0]])
    arbfloat = array([[0.1,3.2],[1.2,0.7]])

    large = identity(10)
    t = large[1,:].copy()
    large[1,:] = large[0,:]
    large[0,:] = t

    def test_large_power(self):
        assert_equal(matrix_power(self.R90,2L**100+2**10+2**5+1),self.R90)

    def test_large_power_trailing_zero(self):
        assert_equal(matrix_power(self.R90,2L**100+2**10+2**5),identity(2))

    def testip_zero(self):
        def tz(M):
            mz = matrix_power(M,0)
            assert_equal(mz, identity(M.shape[0]))
            assert_equal(mz.dtype, M.dtype)
        for M in [self.Arb22, self.arbfloat, self.large]:
            yield tz, M

    def testip_one(self):
        def tz(M):
            mz = matrix_power(M,1)
            assert_equal(mz, M)
            assert_equal(mz.dtype, M.dtype)
        for M in [self.Arb22, self.arbfloat, self.large]:
            yield tz, M

    def testip_two(self):
        def tz(M):
            mz = matrix_power(M,2)
            assert_equal(mz, dot(M,M))
            assert_equal(mz.dtype, M.dtype)
        for M in [self.Arb22, self.arbfloat, self.large]:
            yield tz, M

    def testip_invert(self):
        def tz(M):
            mz = matrix_power(M,-1)
            assert_almost_equal(identity(M.shape[0]), dot(mz,M))
        for M in [self.R90, self.Arb22, self.arbfloat, self.large]:
            yield tz, M

    def test_invert_noninvertible(self):
        import numpy.linalg
        self.assertRaises(numpy.linalg.linalg.LinAlgError,
                lambda: matrix_power(self.noninv,-1))

class TestBoolPower(TestCase):
    def test_square(self):
        A = array([[True,False],[True,True]])
        assert_equal(matrix_power(A,2),A)


class HermitianTestCase(object):
    def test_single(self):
        a = array([[1.,2.], [2.,1.]], dtype=single)
        self.do(a)

    def test_double(self):
        a = array([[1.,2.], [2.,1.]], dtype=double)
        self.do(a)

    def test_csingle(self):
        a = array([[1.,2+3j], [2-3j,1]], dtype=csingle)
        self.do(a)

    def test_cdouble(self):
        a = array([[1.,2+3j], [2-3j,1]], dtype=cdouble)
        self.do(a)

    def test_empty(self):
        a = atleast_2d(array([], dtype = double))
        assert_raises(linalg.LinAlgError, self.do, a)

    def test_nonarray(self):
        a = [[1,2], [2,1]]
        self.do(a)

    def test_matrix_b_only(self):
        """Check that matrix type is preserved."""
        a = array([[1.,2.], [2.,1.]])
        self.do(a)

    def test_matrix_a_and_b(self):
        """Check that matrix type is preserved."""
        a = matrix([[1.,2.], [2.,1.]])
        self.do(a)

class TestEigvalsh(HermitianTestCase, TestCase):
    def do(self, a):
        # note that eigenvalue arrays must be sorted since
        # their order isn't guaranteed.
        ev = linalg.eigvalsh(a)
        evalues, evectors = linalg.eig(a)
        ev.sort()
        evalues.sort()
        assert_almost_equal(ev, evalues)

class TestEigh(HermitianTestCase, TestCase):
    def do(self, a):
        # note that eigenvalue arrays must be sorted since
        # their order isn't guaranteed.
        ev, evc = linalg.eigh(a)
        evalues, evectors = linalg.eig(a)
        ev.sort()
        evalues.sort()
        assert_almost_equal(ev, evalues)

class _TestNorm(TestCase):
    dt = None
    dec = None
    def test_empty(self):
        assert_equal(norm([]), 0.0)
        assert_equal(norm(array([], dtype=self.dt)), 0.0)
        assert_equal(norm(atleast_2d(array([], dtype=self.dt))), 0.0)

    def test_vector(self):
        a = [1.0,2.0,3.0,4.0]
        b = [-1.0,-2.0,-3.0,-4.0]
        c = [-1.0, 2.0,-3.0, 4.0]

        def _test(v):
            np.testing.assert_almost_equal(norm(v), 30**0.5, decimal=self.dec)
            np.testing.assert_almost_equal(norm(v,inf), 4.0, decimal=self.dec)
            np.testing.assert_almost_equal(norm(v,-inf), 1.0, decimal=self.dec)
            np.testing.assert_almost_equal(norm(v,1), 10.0, decimal=self.dec)
            np.testing.assert_almost_equal(norm(v,-1), 12.0/25,
                    decimal=self.dec)
            np.testing.assert_almost_equal(norm(v,2), 30**0.5,
                    decimal=self.dec)
            np.testing.assert_almost_equal(norm(v,-2), ((205./144)**-0.5),
                    decimal=self.dec)

        for v in (a, b, c,):
            _test(v)

        for v in (array(a, dtype=self.dt), array(b, dtype=self.dt),
                  array(c, dtype=self.dt)):
            _test(v)

    def test_matrix(self):
        A = matrix([[1.,3.],[5.,7.]], dtype=self.dt)
        A = matrix([[1.,3.],[5.,7.]], dtype=self.dt)
        assert_almost_equal(norm(A), 84**0.5)
        assert_almost_equal(norm(A,'fro'), 84**0.5)
        assert_almost_equal(norm(A,inf), 12.0)
        assert_almost_equal(norm(A,-inf), 4.0)
        assert_almost_equal(norm(A,1), 10.0)
        assert_almost_equal(norm(A,-1), 6.0)
        assert_almost_equal(norm(A,2), 9.1231056256176615)
        assert_almost_equal(norm(A,-2), 0.87689437438234041)

        self.assertRaises(ValueError, norm, A, 'nofro')
        self.assertRaises(ValueError, norm, A, -3)

class TestNormDouble(_TestNorm):
    dt = np.double
    dec= 12

class TestNormSingle(_TestNorm):
    dt = np.float32
    dec = 6

if __name__ == "__main__":
    run_module_suite()
