import pandas as pd
import uuid
from datetime import datetime, timedelta

def generate_test_data():
    """生成测试用的邮件数据"""
    
    # 创建一些邮箱地址
    emails = {
        'sender1': 'john@company.com',
        'sender2': 'alice@company.com',
        'sender3': 'mike@othercompany.com',
        'receiver1': 'bob@client.com',
        'receiver2': 'carol@client.com',
        'receiver3': 'dave@client.com',
        'receiver4': 'eve@anotherclient.com',
        'receiver5': 'frank@anotherclient.com',
        'invalid': 'invalid-email'  # 无效邮箱，用于测试错误处理
    }
    
    # 创建基本时间点
    base_time = datetime.now()
    
    # 生成测试数据列表
    data = []
    
    # 场景1：简单的原始邮件和回复
    message_id1 = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': 'Project Update',
        '发件人': emails['sender1'],
        '收件人': emails['receiver1'],
        '邮件消息标识': message_id1,
        '创建日期': base_time
    })
    
    # 回复1
    data.append({
        '邮件名称': 'Re: Project Update',
        '发件人': emails['receiver1'],
        '收件人': emails['sender1'],
        '邮件消息标识': f"<{uuid.uuid4()}@client.com>",
        '创建日期': base_time + timedelta(hours=2)
    })
    
    # 场景2：多个回复者来自同一域名
    message_id2 = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': 'Meeting Schedule',
        '发件人': emails['sender1'],
        '收件人': f"{emails['receiver1']},{emails['receiver2']},{emails['receiver3']}",
        '邮件消息标识': message_id2,
        '创建日期': base_time + timedelta(days=1)
    })
    
    # 回复1
    data.append({
        '邮件名称': 'Re: Meeting Schedule',
        '发件人': emails['receiver1'],
        '收件人': emails['sender1'],
        '邮件消息标识': f"<{uuid.uuid4()}@client.com>",
        '创建日期': base_time + timedelta(days=1, hours=3)
    })
    
    # 回复2
    data.append({
        '邮件名称': 'Re: Meeting Schedule',
        '发件人': emails['receiver2'],
        '收件人': emails['sender1'],
        '邮件消息标识': f"<{uuid.uuid4()}@client.com>",
        '创建日期': base_time + timedelta(days=1, hours=4)
    })
    
    # 场景3：多个回复者来自不同域名
    message_id3 = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': 'Partnership Proposal',
        '发件人': emails['sender2'],
        '收件人': f"{emails['receiver1']},{emails['receiver4']}",
        '邮件消息标识': message_id3,
        '创建日期': base_time + timedelta(days=2)
    })
    
    # 回复1
    data.append({
        '邮件名称': 'Re: Partnership Proposal',
        '发件人': emails['receiver1'],
        '收件人': emails['sender2'],
        '邮件消息标识': f"<{uuid.uuid4()}@client.com>",
        '创建日期': base_time + timedelta(days=2, hours=2)
    })
    
    # 回复2
    data.append({
        '邮件名称': 'Re: Partnership Proposal',
        '发件人': emails['receiver4'],
        '收件人': emails['sender2'],
        '邮件消息标识': f"<{uuid.uuid4()}@anotherclient.com>",
        '创建日期': base_time + timedelta(days=2, hours=3)
    })
    
    # 场景4：中文主题和回复前缀
    message_id4 = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': '项目进度报告',
        '发件人': emails['sender3'],
        '收件人': emails['receiver5'],
        '邮件消息标识': message_id4,
        '创建日期': base_time + timedelta(days=3)
    })
    
    # 回复1
    data.append({
        '邮件名称': '回复: 项目进度报告',
        '发件人': emails['receiver5'],
        '收件人': emails['sender3'],
        '邮件消息标识': f"<{uuid.uuid4()}@anotherclient.com>",
        '创建日期': base_time + timedelta(days=3, hours=5)
    })
    
    # 场景5：多个同主题的原始邮件
    message_id5a = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': 'Weekly Report',
        '发件人': emails['sender1'],
        '收件人': emails['receiver1'],
        '邮件消息标识': message_id5a,
        '创建日期': base_time + timedelta(days=4)
    })
    
    # 另一个同主题的原始邮件
    message_id5b = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': 'Weekly Report',
        '发件人': emails['sender2'],
        '收件人': emails['receiver2'],
        '邮件消息标识': message_id5b,
        '创建日期': base_time + timedelta(days=4, hours=1)
    })
    
    # 回复
    data.append({
        '邮件名称': 'Re: Weekly Report',
        '发件人': emails['receiver1'],
        '收件人': emails['sender1'],
        '邮件消息标识': f"<{uuid.uuid4()}@client.com>",
        '创建日期': base_time + timedelta(days=4, hours=2)
    })
    
    # 场景6：无效邮箱格式
    message_id6 = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': 'Important Notice',
        '发件人': emails['sender1'],
        '收件人': emails['invalid'],
        '邮件消息标识': message_id6,
        '创建日期': base_time + timedelta(days=5)
    })
    
    # 回复，但发件人格式无效
    data.append({
        '邮件名称': 'Re: Important Notice',
        '发件人': emails['invalid'],
        '收件人': emails['sender1'],
        '邮件消息标识': f"<{uuid.uuid4()}@unknown>",
        '创建日期': base_time + timedelta(days=5, hours=1)
    })
    
    # 场景7：空主题
    message_id7 = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': '',
        '发件人': emails['sender3'],
        '收件人': emails['receiver3'],
        '邮件消息标识': message_id7,
        '创建日期': base_time + timedelta(days=6)
    })
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    return df

# 用于测试的辅助函数
def create_test_chunks(df, chunk_size=3):
    """将测试数据分成多个块"""
    return [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)] 