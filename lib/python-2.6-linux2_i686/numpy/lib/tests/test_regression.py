from numpy.testing import *
import numpy as np


class TestRegression(object):
    def test_polyfit_build(self):
        """Ticket #628"""
        ref = [-1.06123820e-06, 5.70886914e-04, -1.13822012e-01,
                9.95368241e+00, -3.14526520e+02]
        x = [90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103,
             104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
             116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 129,
             130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141,
             146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157,
             158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169,
             170, 171, 172, 173, 174, 175, 176]
        y = [9.0, 3.0, 7.0, 4.0, 4.0, 8.0, 6.0, 11.0, 9.0, 8.0, 11.0, 5.0,
             6.0, 5.0, 9.0, 8.0, 6.0, 10.0, 6.0, 10.0, 7.0, 6.0, 6.0, 6.0,
             13.0, 4.0, 9.0, 11.0, 4.0, 5.0, 8.0, 5.0, 7.0, 7.0, 6.0, 12.0,
             7.0, 7.0, 9.0, 4.0, 12.0, 6.0, 6.0, 4.0, 3.0, 9.0, 8.0, 8.0,
             6.0, 7.0, 9.0, 10.0, 6.0, 8.0, 4.0, 7.0, 7.0, 10.0, 8.0, 8.0,
             6.0, 3.0, 8.0, 4.0, 5.0, 7.0, 8.0, 6.0, 6.0, 4.0, 12.0, 9.0,
             8.0, 8.0, 8.0, 6.0, 7.0, 4.0, 4.0, 5.0, 7.0]
        tested = np.polyfit(x, y, 4)
        assert_array_almost_equal(ref, tested)

    def test_polyint_type(self) :
        """Ticket #944"""
        msg = "Wrong type, should be complex"
        x = np.ones(3, dtype=np.complex)
        assert_(np.polyint(x).dtype == np.complex, msg)
        msg = "Wrong type, should be float"
        x = np.ones(3, dtype=np.int)
        assert_(np.polyint(x).dtype == np.float, msg)

    def test_polydiv_type(self) :
        """Make polydiv work for complex types"""
        msg = "Wrong type, should be complex"
        x = np.ones(3, dtype=np.complex)
        q,r = np.polydiv(x,x)
        assert_(q.dtype == np.complex, msg)
        msg = "Wrong type, should be float"
        x = np.ones(3, dtype=np.int)
        q,r = np.polydiv(x,x)
        assert_(q.dtype == np.float, msg)


if __name__ == "__main__":
    run_module_suite()
