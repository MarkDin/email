# 邮件关系分析工具

## 简介
这个工具用于分析Excel文件中的邮件数据，识别邮件间的关系，并构建关系集合。

## 功能
- 标准化邮件主题，移除回复前缀
- 识别原始邮件和回复邮件的关系
- 按照发件人、主题和回复者域名构建关系集合
- 记录和保存处理过程中的异常数据

## 使用方法
1. 安装依赖:
```bash
pip install -r requirements.txt
```

2. 修改`main.py`中的输入文件路径:
```python
input_file = "邮件数据.xlsx"  # 更改为实际的Excel文件路径
```

3. 运行程序:
```bash
python main.py
```

4. 查看输出文件:
- `relationships.json`: 关系集合结果
- `error.json`: 异常数据记录

## 文件结构
- `main.py`: 主程序入口
- `email_analyzer.py`: 邮件关系分析核心类
- `utils.py`: 工具函数
- `config.py`: 配置信息
- `requirements.txt`: 依赖包列表

## 注意事项
- Excel文件必须包含以下列: 邮件名称, 发件人, 收件人, 邮件消息标识
- 对于大型文件，程序会自动分块处理以节省内存
- 所有异常数据会被记录并保存到error.json 