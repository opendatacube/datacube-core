"""Utilities shared by all drivers."""



class DriverUtils(object):
    """Constants shared by all drivers."""

    EPSILON = {
        'x': 0.00000001,
        'y': 0.00000001,
        'time': 0.00000001,
        'default': 0.00000001
    }
    '''Precision margins allowed when comparing coord or time
    intervals. For example, to determine whether a coord is regular,
    or to determine an irregular index from its timestamp. Access them
    through the :meth:`epsilon` property.
    '''

    @staticmethod
    def epsilon(dimension):
        """Precision margins allowed when comparing dimensions.

        The comparison may be to determine whether a coord is regular,
        or to determine an irregular index from its timestamp, for
        example.

        :param str dimension: The name of the dimension for which to
          fetch an epsilon.
        :return: Float value, presumably small. A default value is
          returned if the dimension is unknown.
        """
        return DriverUtils.EPSILON[dimension] if dimension in DriverUtils.EPSILON \
            else DriverUtils.EPSILON['default']
