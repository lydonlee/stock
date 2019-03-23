
# encoding: UTF-8


import os
import sys

# 将根目录路径添加到环境变量中
ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
sys.path.append(ROOT_PATH)

# 将功能模块的目录路径添加到环境变量中
# 若各目录下存在同名文件可能导致异常，请注意测试
MODULE_PATH = {}
MODULE_PATH['backtests'] = os.path.join(ROOT_PATH, 'backtests')
MODULE_PATH['module'] = os.path.join(ROOT_PATH, 'module')
MODULE_PATH['config'] = os.path.join(ROOT_PATH, 'config')
MODULE_PATH['data'] = os.path.join(ROOT_PATH, 'data')

# 添加到环境变量中
for path in MODULE_PATH.values():
    if path not in sys.path:
        sys.path.append(path)

if __name__ == '__main__':
    print (ROOT_PATH)
    print(MODULE_PATH)