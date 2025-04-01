#!/usr/bin/env python3
"""
运行测试脚本，直接使用生成的测试数据进行分析。
无需Excel文件，直接测试邮件关系分析算法。
"""

import os
import sys
import json
import logging
import pandas as pd
import uuid
from datetime import datetime, timedelta

# 设置日志
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# 所需配置
REQUIRED_COLUMNS = ['邮件名称', '发件人', '收件人', '邮件消息标识', '创建日期']
DEFAULT_OUTPUT_FILE = "relationships.json"
DEFAULT_ERROR_FILE = "error.json"

# 多语言回复前缀
REPLY_PREFIXES = [
    r"^Re:\s*", r"^回复:\s*", r"^AW:\s*", r"^返信:\s*",
    r"^Rép:\s*", r"^RV:\s*", r"^R:\s*", r"^Res:\s*",
    r"^RE:\s*", r"^回覆:\s*", r"^答复:\s*", r"^返:\s*"
]

# 编译正则表达式以提高性能
import re
PREFIX_PATTERNS = [re.compile(prefix, re.IGNORECASE) for prefix in REPLY_PREFIXES]

def normalize_subject(subject):
    """标准化邮件主题，移除所有回复前缀"""
    if not isinstance(subject, str):
        return ""
        
    original_subject = subject.strip()
    changed = True
    
    while changed:
        changed = False
        for pattern in PREFIX_PATTERNS:
            if pattern.match(original_subject):
                original_subject = pattern.sub("", original_subject).strip()
                changed = True
    
    # 允许空主题，用于测试错误处理
    return original_subject

def extract_domain(email):
    """提取邮箱域名"""
    try:
        # 断言：邮箱地址应该包含@符号
        assert isinstance(email, str) and '@' in email, f"无效的邮箱地址: {email}"
        return email.split('@')[1]
    except (IndexError, AttributeError, AssertionError) as e:
        return None

def extract_username(email):
    """提取邮箱用户名"""
    try:
        # 断言：邮箱地址应该包含@符号
        assert isinstance(email, str) and '@' in email, f"无效的邮箱地址: {email}"
        return email.split('@')[0]
    except (IndexError, AttributeError, AssertionError) as e:
        return None

def is_reply(subject):
    """判断是否为回复邮件"""
    if not isinstance(subject, str):
        return False
        
    for pattern in PREFIX_PATTERNS:
        if pattern.match(subject.strip()):
            return True
    return False

def generate_test_data(more_complex=True):
    """生成测试用的邮件数据，more_complex参数控制是否生成更复杂的测试场景"""
    
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
        '收件人': f"{emails['receiver1']};{emails['receiver2']};{emails['receiver3']}",
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
        '收件人': f"{emails['receiver1']};{emails['receiver4']}",
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
    
    # 另一个同主题的原始邮件，但是时间更早
    message_id5b = f"<{uuid.uuid4()}@company.com>"
    data.append({
        '邮件名称': 'Weekly Report',
        '发件人': emails['sender2'],
        '收件人': emails['receiver2'],
        '邮件消息标识': message_id5b,
        '创建日期': base_time + timedelta(days=3, hours=22)  # 比第一个早了2小时
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
    
    # 添加更复杂的测试场景
    if more_complex:
        # 场景8：多层次回复
        message_id8 = f"<{uuid.uuid4()}@company.com>"
        data.append({
            '邮件名称': 'Deep Thread Test',
            '发件人': emails['sender1'],
            '收件人': emails['receiver1'],
            '邮件消息标识': message_id8,
            '创建日期': base_time + timedelta(days=7)
        })
        
        # 回复1
        reply_id1 = f"<{uuid.uuid4()}@client.com>"
        data.append({
            '邮件名称': 'Re: Deep Thread Test',
            '发件人': emails['receiver1'],
            '收件人': emails['sender1'],
            '邮件消息标识': reply_id1,
            '创建日期': base_time + timedelta(days=7, hours=1)
        })
        
        # 回复的回复
        data.append({
            '邮件名称': 'Re: Re: Deep Thread Test',
            '发件人': emails['sender1'],
            '收件人': emails['receiver1'],
            '邮件消息标识': f"<{uuid.uuid4()}@company.com>",
            '创建日期': base_time + timedelta(days=7, hours=2)
        })
        
        # 回复的回复的回复
        data.append({
            '邮件名称': 'Re: Re: Re: Deep Thread Test',
            '发件人': emails['receiver1'],
            '收件人': emails['sender1'],
            '邮件消息标识': f"<{uuid.uuid4()}@client.com>",
            '创建日期': base_time + timedelta(days=7, hours=3)
        })
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    return df

class EmailRelationshipAnalyzer:
    def __init__(self, excel_file_path=None):
        """初始化分析器"""
        self.excel_file_path = excel_file_path or "测试数据"
        self.relationships = {}  # 存储关系集合
        self.errors = {}  # 存储异常数据
        self.required_columns = REQUIRED_COLUMNS
    
    def process_subject_group(self, subject, group):
        """处理单个主题组的邮件"""
        try:
            # 找出原始邮件（非回复）和回复邮件
            original_emails = group[~group['is_reply']]
            reply_emails = group[group['is_reply']]
            
            # 断言：分组后的数据不应该为空
            assert not group.empty, f"主题为'{subject}'的分组为空"
            
            if original_emails.empty:
                # 如果没有原始邮件，记录错误
                for _, row in group.iterrows():
                    self.errors[row['邮件消息标识']] = "找不到对应的原始邮件，全部都是回复"
                return
            
            # 处理多个原始邮件的情况 - 按时间排序选择最早的一个
            if len(original_emails) > 1:
                # 检查是否有创建日期列
                if '创建日期' in original_emails.columns:
                    original_emails = original_emails.sort_values('创建日期')
                    logger.warning(f"主题'{subject}'有多个原始邮件，使用最早的一个")
                else:
                    logger.warning(f"主题'{subject}'有多个原始邮件，使用第一个")
                
                # 记录未使用的原始邮件
                for _, row in original_emails.iloc[1:].iterrows():
                    self.errors[row['邮件消息标识']] = "存在多个同主题的原始邮件，已使用其他邮件作为原始邮件"
            
            original_email = original_emails.iloc[0]
            sender = original_email['发件人']
            
            # 验证发件人格式
            if not isinstance(sender, str) or '@' not in sender:
                self.errors[original_email['邮件消息标识']] = f"发件人格式不正确: {sender}"
                return
            
            # 处理回复邮件
            for _, reply in reply_emails.iterrows():
                replier = reply['发件人']
                
                # 验证回复者格式
                if not isinstance(replier, str) or '@' not in replier:
                    self.errors[reply['邮件消息标识']] = f"回复者格式不正确: {replier}"
                    continue
                
                # 提取回复者域名
                reply_domain = extract_domain(replier)
                if not reply_domain:
                    self.errors[reply['邮件消息标识']] = f"无法提取回复者域名: {replier}"
                    continue
                
                # 断言：域名不应该为空
                assert reply_domain, f"提取的域名为空: {replier}"
                
                # 构建关系键
                key = f"{sender}.{subject}.{reply_domain}"
                
                # 构建关系值
                username = extract_username(replier)
                
                # 断言：用户名不应该为空
                assert username, f"提取的用户名为空: {replier}"
                
                value = (sender, subject, username)
                
                # 添加到关系集合
                if key not in self.relationships:
                    self.relationships[key] = []
                
                if value not in self.relationships[key]:
                    self.relationships[key].append(value)
                    
        except AssertionError as e:
            # 记录断言错误
            logger.error(f"断言错误(主题'{subject}'): {str(e)}")
            self.errors[f"subject_{subject}"] = f"断言错误: {str(e)}"
        
        except Exception as e:
            # 记录其他错误
            logger.error(f"处理主题'{subject}'时发生错误: {str(e)}")
            self.errors[f"subject_{subject}"] = f"处理错误: {str(e)}"
    
    def run_analysis_on_test_data(self):
        """在测试数据上运行分析"""
        logger.info("开始分析测试数据")
        
        # 生成测试数据
        test_data = generate_test_data(more_complex=True)
        logger.info(f"生成了 {len(test_data)} 条测试数据")
        
        # 标准化主题和识别回复
        test_data['normalized_subject'] = test_data['邮件名称'].apply(lambda x: normalize_subject(x) if pd.notna(x) else "")
        test_data['is_reply'] = test_data['邮件名称'].apply(lambda x: is_reply(x) if pd.notna(x) else False)
        
        # 按标准化主题分组
        unique_subjects = test_data['normalized_subject'].unique()
        logger.info(f"共有 {len(unique_subjects)} 个不同的主题")
        
        # 遍历每个主题分组处理
        for subject in unique_subjects:
            if not subject:  # 跳过空主题
                continue
                
            group = test_data[test_data['normalized_subject'] == subject]
            self.process_subject_group(subject, group)
        
        # 检查是否找到了关系
        if not self.relationships:
            logger.warning("未找到任何关系集合")
        else:
            logger.info(f"分析完成，找到 {len(self.relationships)} 个关系集合，记录 {len(self.errors)} 个异常")
        
        return self.relationships, self.errors
    
    def save_relationships(self, output_file):
        """保存关系集合到JSON文件"""
        try:
            # 检查关系集合是否为空
            if not self.relationships:
                logger.warning("关系集合为空，没有内容可保存")
                return
            
            # 转换格式并按count排序
            formatted_relationships = {}
            
            # 1. 转换格式: 添加count字段和items字段
            for key, items in self.relationships.items():
                formatted_relationships[key] = {
                    "count": len(items),
                    "items": items
                }
            
            # 2. 按count从大到小排序
            sorted_relationships = dict(
                sorted(
                    formatted_relationships.items(),
                    key=lambda x: x[1]["count"],
                    reverse=True
                )
            )
            
            # 保存到JSON文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(sorted_relationships, f, ensure_ascii=False, indent=2)
            
            logger.info(f"关系集合已保存到: {output_file}")
        except Exception as e:
            logger.error(f"保存关系集合时出错: {str(e)}")
            self.errors["save_relationships"] = f"保存关系集合错误: {str(e)}"
    
    def save_errors(self, error_file):
        """保存异常数据到JSON文件"""
        try:
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump(self.errors, f, ensure_ascii=False, indent=2)
            logger.info(f"异常数据已保存到: {error_file}")
        except Exception as e:
            logger.error(f"保存异常数据时出错: {str(e)}")

def main():
    """运行测试"""
    try:
        # 初始化测试分析器
        analyzer = EmailRelationshipAnalyzer()
        
        # 运行分析
        relationships, errors = analyzer.run_analysis_on_test_data()
        
        # 保存结果
        analyzer.save_relationships(DEFAULT_OUTPUT_FILE)
        analyzer.save_errors(DEFAULT_ERROR_FILE)
        
        # 显示结果
        print("\n=== 找到的关系集合 ===")
        # 按照count从大到小排序显示
        formatted_relationships = {}
        for key, items in relationships.items():
            formatted_relationships[key] = {
                "count": len(items),
                "items": items
            }
        
        sorted_relationships = sorted(
            formatted_relationships.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )
        
        for key, data in sorted_relationships:
            print(f"\n{key} (数量: {data['count']}):")
            for item in data['items']:
                print(f"  - {item}")
        
        print("\n=== 记录的错误 ===")
        for key, value in errors.items():
            print(f"{key}: {value}")
        
        logger.info("测试完成!")
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        # 如果分析器已经初始化，尝试保存已收集的错误
        if 'analyzer' in locals():
            analyzer.save_errors(DEFAULT_ERROR_FILE)

if __name__ == "__main__":
    main() 