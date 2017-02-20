import sys
import runpy

sys.argv.pop(0)
runpy.run_module(sys.argv[0], run_name="__main__", alter_sys=True)
