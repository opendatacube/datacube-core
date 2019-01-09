from datacube.execution import worker

import warnings


def main():
    warnings.warn('datacube_apps.worker should now '
                  'be imported as datacube.execution.worker',
                  DeprecationWarning, stacklevel=2)
    return worker.main()


if __name__ == '__main__':
    main()
