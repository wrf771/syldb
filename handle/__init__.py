import os
from syldb.tools.fileTools import join_path
from syldb.handle.configHandle import ConfigHandle


def init():
    # 构造默认配置文件路径
    path = join_path(os.getcwd(), 'syldb', 'conf')

    # 实例化配置文件操作对象
    _ = ConfigHandle(path, file_name='syldb.ini')


if __name__ != '__main__':
    # 执行初始化操作
    init()
