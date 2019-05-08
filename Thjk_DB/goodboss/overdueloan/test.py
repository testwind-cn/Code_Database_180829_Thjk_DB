

import configparser

config = configparser.ConfigParser()
config.read('config/data_000.cfg')

a_float = config.getfloat('Mysql-01', 'aa')
an_int = config.getint('Mysql-02', 'bb')
bn_int = config.get('Mysql-02', 'bb', fallback=999)
cn_int = config.get('Mysql-01', 'ff',fallback=9919)
dn_int = config.getfloat('Mysql-02', 'ff',fallback=9991)
# en_int = config.getfloat('Mysql-01', 'ff',fallback=9919)
en_int = config.get('Mysql-02', 'ee',raw=False,fallback=9919)

en_int = config.get('D0009', 'data_type',raw=True,fallback=None)
bn_int = config.get('D0009', 's_sql',raw=True,fallback=None)
fn_int = config.get('D0009', 's_sql2',raw=True,fallback=None)

print(a_float + an_int)

# 1、 如果变量在本节没有，会优先使用 DEFAULT 节的内容，
# 2、 如果 DEFAULT 节也没有，才使用 fallback 里的内容
# 3、 getfloat 的转换会出现类型错误，要自己控制
# 4、 ConfigParser 支持%(bar)变量转换，见下面例子。
# 5、 raw=False 使用转换、raw=True 不使用转换
# 6、 %(bar)的变量转换，要求变量在本节、缺省节要存在，不存在会出错。

# config.set('Section1', 'baz', 'fun')
# config.set('Section1', 'bar', 'Python')
# config.set('Section1', 'foo', '%(bar)s is %(baz)s!')
# Set the optional *raw* argument of get() to True if you wish to disable
# interpolation in a single get operation.
# print(cfg.get('Section1', 'foo', raw=False))  # -> "Python is fun!"
# print(cfg.get('Section1', 'foo', raw=True))   # -> "%(bar)s is %(baz)s!"
