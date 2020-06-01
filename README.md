# 概要
1.原始数据入库  
2.算法数据输出  
3.算法结果返回  


# 依赖包
os、re、pymysql、openpyxl


# 说明及要求
## 程序使用说明
1.在本地安装依赖包  
2.在\package\config_file.py中配置数据库信息  
3.运行\run.py  

## 入库文件格式要求
1.除一分一段表外，其他文件名要由 “省份 + 年份 + 关键字” 构成，关键字为["录取分数","拆分表","一分一段表","报考书","数据对标"]  
2.程序与入库文件放在同一目录下  
3.一分一段表字段名按关键字匹配，其他表字段名不可变更，顺序可变  
4.sys_ batch暂时没有做判断，有新增省份要先手动维护  
5.对标表，文理两个子表里都没有数据才会上传  


# TODO
## 功能
- [ ] 算法数据输出 
- [ ] 算法结果返回
## 非功能性
- [ ] 优化


# 问题反馈
cbowen-yy@163.com
