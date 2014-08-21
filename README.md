RF_DBHelper
===========
关于RobotFramework的介绍，请前往其官网http://robotframework.org/

RF支持三种测试库：
1 内置库
2 外部库
3 自定义库（理论上RF支持所有语言封装的测试库）

RF的测试库也可以通过普通的JAVA MAIN函数来测试，实际上关键字其实就是一个个“测试函数名”，在RF中，需要用doclibtool.py来生成一个RIDE能够识别的测试关键字xml文件导入
